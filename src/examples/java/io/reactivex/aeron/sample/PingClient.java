package io.reactivex.aeron.sample;

import io.reactivex.aeron.RxAeron;
import io.reactivex.aeron.requestreply.RequestReplyClient;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/12/15.
 */
public class PingClient {
    private static final String PING_SERVER_CHANNEL = "aeron:udp?remote=localhost:" + 50_000;
    private static final String PONG_RESPONSE_CHANNEL = "aeron:udp?remote=localhost:" + 50_001;

    public static void main(String... args) {
        RxAeron rxAeron = RxAeron.getInstance();
        RequestReplyClient requestReplyClient = rxAeron.createRequestReplyClient(PING_SERVER_CHANNEL, PONG_RESPONSE_CHANNEL);

        Observable<DirectBuffer> buffer = Observable
            .interval(1, TimeUnit.NANOSECONDS)
            .map(i -> "ping =>" + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        AtomicLong count = new AtomicLong(0);

        requestReplyClient
            .offer(buffer)
            .doOnNext(p -> {
                long c = count.getAndIncrement();

                if (c % 10_000 == 0) {
                    System.out.println(new String(p.byteArray()));
                }
            })
            .toBlocking().last();
    }
}
