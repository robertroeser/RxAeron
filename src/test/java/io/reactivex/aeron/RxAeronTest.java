package io.reactivex.aeron;

import io.reactivex.aeron.requestreply.RequestReplyClient;
import io.reactivex.aeron.requestreply.RequestReplyServer;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import junit.framework.TestCase;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class RxAeronTest extends TestCase {
    public static final String CHANNEL = "aeron:udp?remote=localhost:43450";

    public static final String SERVER_CHANNEL = "aeron:udp?remote=localhost:50000";

    public static final String RESPONSE_CHANNEL = "aeron:udp?remote=localhost:60000";


    @Test(timeout = 2000)
    public void testUnicastServerAndClientForTen() throws Exception {


        CountDownLatch countDownLatch = new CountDownLatch(10);

        UnicastClient unicastClient = RxAeron.createUnicastClient(CHANNEL);
        UnicastServer unicastServer = RxAeron.createUnicastServer(CHANNEL, (buffer) ->
                buffer.map(b -> {
                    String s = new String(b.byteArray());
                    System.out.println(Thread.currentThread() + " -- handling => " + s);

                    countDownLatch.countDown();

                    return null;
                })
        );


        Observable<DirectBuffer> buffer = Observable
            .range(1, 10)
            .map(i -> "sending_" + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        unicastClient
                .offer(buffer)
                .toBlocking().last();

        countDownLatch.await();

        unicastClient.close();
        unicastServer.close();
    }

    @Test(timeout = 2000)
    public void testUnicastServerAndClientForTenThousand() throws Exception {

        CountDownLatch countDownLatch = new CountDownLatch(10_000);

        UnicastClient unicastClient = RxAeron.createUnicastClient(CHANNEL);
        UnicastServer unicastServer = RxAeron.createUnicastServer(CHANNEL, (buffer) ->
                buffer.map(b -> {

                    countDownLatch.countDown();

                    return null;
                })
        );


        Observable<DirectBuffer> buffer = Observable
            .range(1, 10_000)
            .map(i -> "sending_" + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        unicastClient
            .offer(buffer)
            .toBlocking().last();

        countDownLatch.await();

        unicastClient.close();
        unicastServer.close();
    }

    @Test
    public void testRequestReply() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(10);

        RequestReplyServer requestReplyServer = RxAeron.createRequestReplyServer(SERVER_CHANNEL, new Func1<Observable<DirectBuffer>, Observable<DirectBuffer>>() {
            @Override
            public Observable<DirectBuffer> call(Observable<DirectBuffer> incoming) {
                return incoming
                    .map(i -> {
                        countDownLatch.countDown();
                        String s = new String(i.byteArray(), Charset.defaultCharset());
                        String pong = "Pong => " + s;
                        return new UnsafeBuffer(pong.getBytes());
                    });
            }
        });

        RequestReplyClient requestReplyClient = RxAeron.createRequestReplyClient(SERVER_CHANNEL, RESPONSE_CHANNEL);

        Observable<DirectBuffer> buffer = Observable
            .range(1, 10)
            .map(i -> "ping => " + i)
            .doOnNext(System.out::println)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        requestReplyClient
            .offer(buffer)
            .toBlocking()
            .forEach(db -> {
                String s = new String(db.byteArray(), Charset.defaultCharset());
                System.out.println(s);
            });

        assertEquals(0, countDownLatch.getCount());

        requestReplyClient.close();
        requestReplyServer.close();

    }

    @Test
    public void testRequestReplyFor10KMessages() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(10_000);

        RequestReplyServer requestReplyServer = RxAeron.createRequestReplyServer(SERVER_CHANNEL, new Func1<Observable<DirectBuffer>, Observable<DirectBuffer>>() {
            @Override
            public Observable<DirectBuffer> call(Observable<DirectBuffer> incoming) {
                return incoming
                    .map(i -> {
                        countDownLatch.countDown();
                        String s = new String(i.byteArray(), Charset.defaultCharset());
                        String pong = "Pong => " + s;
                        return new UnsafeBuffer(pong.getBytes());
                    });
            }
        });

        RequestReplyClient requestReplyClient = RxAeron.createRequestReplyClient(SERVER_CHANNEL, RESPONSE_CHANNEL);

        Observable<DirectBuffer> buffer = Observable
            .range(1, 10_000)
            .map(i -> "ping => " + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        requestReplyClient
            .offer(buffer)
            .toBlocking()
            .forEach(db -> {
                new String(db.byteArray(), Charset.defaultCharset());
            });

        assertEquals(0, countDownLatch.getCount());

        requestReplyClient.close();
        requestReplyServer.close();

    }

}