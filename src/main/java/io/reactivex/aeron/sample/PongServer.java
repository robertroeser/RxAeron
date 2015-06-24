package io.reactivex.aeron.sample;

import io.reactivex.aeron.RxAeron;
import rx.Observable;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by rroeser on 6/12/15.
 */
public class PongServer {
    private static final String PING_SERVER_CHANNEL = "aeron:udp?remote=localhost:" + 50_000;

    public static void main(String... args) {
        AtomicLong count = new AtomicLong(0);

        RxAeron.createRequestReplyServer(PING_SERVER_CHANNEL, bufferObservable ->
                bufferObservable
                    .flatMap(buffer -> {
                        final long c = count.incrementAndGet();

                        if (c % 10_000 == 0) {
                            System.out.println(new String(buffer.byteArray()));
                        }

                        String pong = "pong => " + c;
                        return Observable.just(new UnsafeBuffer(pong.getBytes()));
                    })
        ).enableRateReport();

        LockSupport.park();
    }
}
