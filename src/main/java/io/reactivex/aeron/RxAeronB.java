package io.reactivex.aeron;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observable.Operator;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.functions.Func1;
import rx.functions.Func3;
import rx.schedulers.Schedulers;
import rx.subscriptions.Subscriptions;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class RxAeronB {

    private static final String TOMBSTONE = "TOMBSTONE";
    public static final String CHANNEL = "aeron:udp?remote=localhost:43450";

    private static final int MAX_BUFFER_LENGTH = 1024;

    public static void main(String... args) {

        Aeron aeron = start();

        //        /* async data stream with a tombstone to allow unsubscribing on the other end */
        //        Observable<String> data = Observable.interval(100, TimeUnit.MILLISECONDS).map(i -> {
        //            return "data_" + (i + 1);
        //        }).take(5).concatWith(Observable.just(TOMBSTONE));

        /* cold, fast, synchronous data stream that supports backpressure */
        Observable<String> data = Observable.range(1, Integer.MAX_VALUE)
                .doOnNext(i -> {
                    if (i % 100 == 0) {
                        System.out.println("Source Emitted => " + i); // this should not emit beyond consumption
                    }
                })
                .map(i -> "data_" + i)
        //                .doOnRequest(r -> System.out.println("requested: " + r))
        ;

        publish(aeron, CHANNEL, 1, data, RxAeronB::stringToBuffer)
                .doOnError(t -> t.printStackTrace())
                .subscribe();

        //        consume(aeron, RESPONSE_CHANNEL, 1,
        //                (buffer, offset, length) -> buffer.getStringWithoutLengthUtf8(offset, length).trim())
        //                .flatMap(d -> {
        //                    return publish(aeron, RESPONSE_CHANNEL, 1, Observable.just(d), RxAeronExample::stringToBuffer);
        //                });

        consume(aeron, CHANNEL, 1,
                (buffer, offset, length) -> buffer.getStringWithoutLengthUtf8(offset, length).trim())
                .takeWhile(s -> !s.equals(TOMBSTONE))
                .toBlocking().forEach(System.out::println);

        System.out.println("done");
    }

    private static Publication getServerPublication(Aeron aeron, final String channel, final int streamId) {
        return aeron.addPublication(channel, streamId);
    }
    
    /**
     * 
     * @param aeron
     * @param channel
     * @param streamId
     * @param data
     * @param map
     * @return Observable<Long> that emits the new stream position after each Aeron channel emission
     */
    public static <T> Observable<Long> publish(Aeron aeron, final String channel, final int streamId, Observable<T> data, Func1<T, DirectBuffer> map) {

        /* backpressure aware operator for offering to Publication */
        return data.lift(new Operator<Long, T>() {

            @Override
            public Subscriber<? super T> call(Subscriber<? super Long> child) {
                return new Subscriber<T>(child) {

                    private DirectBuffer last;
                    Publication serverPublication;
                    Worker w;

                    @Override
                    public void onStart() {
                        serverPublication = getServerPublication(aeron, channel, streamId);
                        add(Subscriptions.create(() -> serverPublication.close()));
                        w = Schedulers.computation().createWorker();
                        add(w);
                        // TODO make this do more than 1
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        child.onCompleted();
                    }

                    @Override
                    public void onError(Throwable e) {
                        child.onError(e);
                    }

                    @Override
                    public void onNext(T t) {
                        DirectBuffer v = map.call(t);
                        tryOffer(v);
                    }

                    private void tryOffer(DirectBuffer v) {
                        long sent = serverPublication.offer(v);
                        if (sent == Publication.NOT_CONNECTED) {
                            onError(new RuntimeException("Not connected"));
                        } else if (sent == Publication.BACK_PRESSURE) {
                            last = v;
                            w.schedule(() -> tryOffer(v));
                        } else {
                            child.onNext(sent);
                            // TODO make this do more than 1
                            request(1);
                        }
                    }

                };
            }

        });
    }

    public static <T> Observable<T> consume(Aeron aeron, final String channel, final int streamId, Func3<DirectBuffer, Integer, Integer, T> map) {
        return Observable.create(s -> {
            Subscription subscription = aeron.addSubscription(channel, streamId, new FragmentAssemblyAdapter((buffer, offset, length, header) -> {
                T value = map.call(buffer, offset, length);
                try {
                    // make it behave slowly
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                s.onNext(value);
            }));
            s.add(Subscriptions.create(() -> subscription.close()));

            // use existing event loops
                Worker w = Schedulers.computation().createWorker();
                // limit fragments so it doesn't starve the eventloop
                w.schedulePeriodically(() -> subscription.poll(100), 0, 0, TimeUnit.NANOSECONDS);
                s.add(w);
            });
    }

    public static Aeron start() {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        final MediaDriver mediaDriver = MediaDriver.launch(ctx.dirsDeleteOnExit(true));

        final Aeron.Context context = new Aeron.Context()
                .newConnectionHandler((String channel, int streamId, int sessionId, long joiningPosition, String sourceInformation) -> {
                    System.out.println("New Connection => channel: " + channel + " stream: " + streamId + " session: " + sessionId + " position: " + joiningPosition + "  source: " + sourceInformation);
                })
                .errorHandler(t -> t.printStackTrace());
        final Aeron aeron = Aeron.connect(context);
        return aeron;
    }

    // TODO not thread-safe at all for this experiment since it is statically defined
    final static UnsafeBuffer unsafeBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MAX_BUFFER_LENGTH));

    public static DirectBuffer stringToBuffer(String s) {
        byte[] bytes = s.getBytes();
        unsafeBuffer.putBytes(0, bytes);
        return unsafeBuffer;
    }
}
