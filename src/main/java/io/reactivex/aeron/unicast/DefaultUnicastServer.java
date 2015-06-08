package io.reactivex.aeron.unicast;

import io.reactivex.aeron.protocol.MessageHeaderDecoder;
import io.reactivex.aeron.protocol.UnicastRequestDecoder;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.tools.RateReporter;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/5/15.
 */
public class DefaultUnicastServer implements UnicastServer {
    private final Subscription subscription;

    private final ThreadLocal<MessageHeaderDecoder> messageHeaderDecoderLocal = ThreadLocal.withInitial(MessageHeaderDecoder::new);
    private final ThreadLocal<UnicastRequestDecoder> unicastRequestDecoderLocal = ThreadLocal.withInitial(UnicastRequestDecoder::new);

    private final String channel;
    private final int streamId;
    private final Scheduler.Worker worker;

    class Stats implements RateReporter.Stats {
        long verifiableMessages = 0;
        long nonVerifiableMessages = 0;
        long bytes = 0;

        void incrementBytes(long bytes) {
            this.bytes += bytes;
        }

        void incrementVerifiableMessages() {
            verifiableMessages++;
        }

        void incrementNonVerifiableMessages() {
            nonVerifiableMessages++;
        }

        @Override
        public long verifiableMessages() {
            return verifiableMessages;
        }

        @Override
        public long bytes() {
            return bytes;
        }

        @Override
        public long nonVerifiableMessages() {
            return nonVerifiableMessages;
        }
    }

    class DefaultCallback implements RateReporter.Callback
    {
        public void report(final StringBuilder reportString)
        {
            System.out.println(reportString);
        }
    }

    private final Stats stats;

    public DefaultUnicastServer(Aeron aeron, String channel, int streamId, Func1<Observable<DirectBuffer>, Observable<Void>> handle) {
        this.channel = channel;
        this.streamId = streamId;

        stats = new Stats();

        this.subscription = aeron.addSubscription(channel, streamId, (buffer, offset, length, header) -> {
            stats.incrementBytes(length);

            MessageHeaderDecoder messageHeaderDecoder = messageHeaderDecoderLocal.get();
            messageHeaderDecoder.wrap(buffer, offset, 0);

            int templateId = messageHeaderDecoder.templateId();

            if (templateId == UnicastRequestDecoder.TEMPLATE_ID) {
                stats.incrementVerifiableMessages();

                UnicastRequestDecoder unicastRequestDecoder = unicastRequestDecoderLocal.get();
                unicastRequestDecoder.wrap(buffer, offset + messageHeaderDecoder.size(), messageHeaderDecoder.blockLength(), 0);

                byte[] bytes = new byte[unicastRequestDecoder.payloadLength()];
                unicastRequestDecoder.getPayload(bytes, 0, unicastRequestDecoder.payloadLength());

                UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);
                Observable<DirectBuffer> dataObservable = Observable.just(payloadBuffer);
                Observable<Void> handleObservable = handle.call(dataObservable);
                handleObservable.subscribe();
            } else {
                stats.incrementNonVerifiableMessages();
                System.err.println("Unknown template id " + templateId);
            }

        });

        this.worker = Schedulers.computation().createWorker();

        worker.schedulePeriodically(() -> subscription.poll(100), 0, 1, TimeUnit.NANOSECONDS);
    }


    public int getStreamId() {
        return streamId;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public void enableRateReport() {
        RateReporter rateReporter = new RateReporter(stats, new DefaultCallback());

        Thread t = new Thread(rateReporter);
        t.setDaemon(true);
        t.start();

    }

    @Override
    public void close() throws IOException {
        Arrays.asList(worker).forEach(Scheduler.Worker::unsubscribe);
        subscription.close();
    }
}
