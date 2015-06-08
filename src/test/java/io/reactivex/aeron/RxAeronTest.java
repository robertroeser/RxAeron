package io.reactivex.aeron;

import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import junit.framework.TestCase;
import org.junit.Test;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.CountDownLatch;

public class RxAeronTest extends TestCase {
    public static final String CHANNEL = "aeron:udp?remote=localhost:43450";

    private RxAeron instance = RxAeron.getInstance();

    @Test(timeout = 2000)
    public void testUnicastServerAndClientForTen() throws Exception {


        CountDownLatch countDownLatch = new CountDownLatch(10);

        UnicastClient unicastClient = instance.createUnicastClient(CHANNEL);
        UnicastServer unicastServer = instance.createUnicastServer(CHANNEL, (buffer) ->
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

        UnicastClient unicastClient = instance.createUnicastClient(CHANNEL);
        UnicastServer unicastServer = instance.createUnicastServer(CHANNEL, (buffer) ->
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

}