package io.reactivex.aeron.unicast;

import io.reactivex.aeron.protocol.MessageHeaderDecoder;
import io.reactivex.aeron.protocol.UnicastRequestDecoder;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
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

    public DefaultUnicastServer(Aeron aeron, String channel, int streamId, Func1<Observable<DirectBuffer>, Observable<Void>> handle) {
        this.channel = channel;
        this.streamId = streamId;

        this.subscription = aeron.addSubscription(channel, streamId, (buffer, offset, length, header) -> {
            MessageHeaderDecoder messageHeaderDecoder = messageHeaderDecoderLocal.get();
            messageHeaderDecoder.wrap(buffer, offset, 0);

            int templateId = messageHeaderDecoder.templateId();

            if (templateId == UnicastRequestDecoder.TEMPLATE_ID) {
                UnicastRequestDecoder unicastRequestDecoder = unicastRequestDecoderLocal.get();
                unicastRequestDecoder.wrap(buffer, offset + messageHeaderDecoder.size(), messageHeaderDecoder.blockLength(), 0);

                byte[] bytes = new byte[unicastRequestDecoder.payloadLength()];
                unicastRequestDecoder.getPayload(bytes, 0, unicastRequestDecoder.payloadLength());

                UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);
                Observable<DirectBuffer> dataObservable = Observable.just(payloadBuffer);
                Observable<Void> handleObservable = handle.call(dataObservable);
                handleObservable.subscribeOn(Schedulers.computation()).subscribe();
            } else {
                System.err.println("Unknown template id " + templateId);
            }

        });

        this.worker = Schedulers.computation().createWorker();

        worker.schedulePeriodically(() ->  subscription.poll(100) , 0, 0, TimeUnit.NANOSECONDS);
    }


    public int getStreamId() {
        return streamId;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public void close() throws IOException {
        worker.unsubscribe();
        subscription.close();
    }
}
