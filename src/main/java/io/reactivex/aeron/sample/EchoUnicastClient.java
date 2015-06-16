package io.reactivex.aeron.sample;

import io.reactivex.aeron.RxAeron;
import io.reactivex.aeron.unicast.UnicastClient;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/8/15.
 */
public class EchoUnicastClient {
    private static final String ECHO_UNICAST_CHANNEL = "aeron:udp?remote=localhost:43123";

    public static void main(String... args) {
        String channel = System.getProperty("channel", ECHO_UNICAST_CHANNEL);
        System.out.println("Listening on channel " + channel);
        RxAeron rxAeron = RxAeron.getInstance();
        UnicastClient unicastClient = rxAeron.createUnicastClient(channel);

        Observable<DirectBuffer> buffer = Observable
            .interval(1, TimeUnit.NANOSECONDS)
            .map(i -> "sending_" + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        unicastClient
            .offer(buffer)
            .toBlocking().last();
    }
}
