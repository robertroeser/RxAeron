package io.reactivex.aeron.sample;

import io.reactivex.aeron.RxAeron;

import java.util.concurrent.locks.LockSupport;

/**
 * Created by rroeser on 6/8/15.
 */
public class EchoUnicastServer {
    private static final String ECHO_UNICAST_CHANNEL = "aeron:udp?remote=localhost:43123";

    public static void main(String... args) {
        final long[] count = {0};

        RxAeron rxAeron = RxAeron.getInstance();
        rxAeron.createUnicastServer(ECHO_UNICAST_CHANNEL, bufferObservable ->
            bufferObservable
                .map(buffer -> {
                        count[0]++;

                        if (count[0] % 10_000 == 0) {
                            System.out.println(new String(buffer.byteArray()));
                        }

                        return null;
                    })
        ).enableRateReport();

        LockSupport.park();
    }
}
