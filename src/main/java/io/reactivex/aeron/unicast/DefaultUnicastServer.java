package io.reactivex.aeron.unicast;

import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.protocol.MessageHeaderDecoder;
import rx.Observable;
import rx.Scheduler;
import rx.functions.Func3;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.tools.RateReporter;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/5/15.
 */
public class DefaultUnicastServer implements UnicastServer {
    private final Subscription subscription;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    private final String channel;
    private final int streamId;
    private final Scheduler.Worker worker;
    private volatile boolean running = true;


    private static final long IDLE_MAX_SPINS = 100;
    private static final long IDLE_MAX_YIELDS = 10;
    private static final long IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    private static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);

    private final IdleStrategy idleStrategy =
        new BackoffIdleStrategy(IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);

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
            try {
                System.out.println(reportString);
            } catch (Throwable t) {}
        }
    }

    private final Stats stats;

    public DefaultUnicastServer(Aeron aeron,
                                String channel,
                                int streamId,
                                Long2ObjectHashMap<SubscriptionDataHandler> handlers) {
        this.channel = channel;
        this.streamId = streamId;

        stats = new Stats();

        this.subscription = aeron.addSubscription(channel, streamId, new FragmentAssemblyAdapter(
            (buffer, offset, length, header) -> {
            stats.incrementBytes(length);

            messageHeaderDecoder.wrap(buffer, offset, 0);

            int templateId = messageHeaderDecoder.templateId();

            Func3<DirectBuffer, Integer, Integer, Observable<Void>> handler = handlers.get(templateId);

            if (handler != null) {
                stats.incrementVerifiableMessages();
                Observable<Void> call =
                    handler.call(buffer, offset + messageHeaderDecoder.size(), messageHeaderDecoder.blockLength());
                call.doOnError(Throwable::printStackTrace).subscribe();
            } else {
                stats.incrementNonVerifiableMessages();
                System.err.println("Unknown template id " + templateId);
            }
        }));

        this.worker = Schedulers.newThread().createWorker();

        worker.schedule(() -> {
            do {
               try {

                   int fragments = subscription.poll(Integer.MAX_VALUE);

                   idleStrategy.idle(fragments);
               } catch(Throwable t) {
                   System.err.println("Exception occurred on channel " + channel + " on stream id " + streamId);
                    t.printStackTrace();
               }
           } while(running);
        });
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
        running = false;
        worker.unsubscribe();
        subscription.close();
    }
}
