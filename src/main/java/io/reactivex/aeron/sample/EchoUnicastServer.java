package io.reactivex.aeron.sample;

import io.reactivex.aeron.RxAeron;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by rroeser on 6/8/15.
 */
public class EchoUnicastServer {
    private static final String ECHO_UNICAST_CHANNEL = "aeron:udp?remote=localhost:43123";

    public static void main(String... args) {
        String channel = System.getProperty("channel", ECHO_UNICAST_CHANNEL);
        System.out.println("Listening on channel " + channel);
        final AtomicLong count = new AtomicLong();

        RxAeron.createUnicastServer(channel, bufferObservable ->
            bufferObservable
                .map(buffer -> {
                        long c = count.incrementAndGet();

                        if (c % 10_000 == 0) {
                            System.out.println(new String(buffer.byteArray()));
                        }

                        return null;
                    })
        ).enableRateReport();

        LockSupport.park();
    }
}
