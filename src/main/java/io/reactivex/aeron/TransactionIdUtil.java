package io.reactivex.aeron;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/4/15.
 */
public class TransactionIdUtil {
    private static AtomicLong id = new AtomicLong(0);

    private static Random rnd = new SecureRandom();

    public static long getTransactionId() {
        return id.incrementAndGet();
    }

    public static long getConnectionId(String channel) {
        long hash = 5381;

        for (int i = 0; i < channel.length(); i++) {
            hash = ((hash << 5) + hash) + channel.charAt(i);
        }

        return hash;
    }
}