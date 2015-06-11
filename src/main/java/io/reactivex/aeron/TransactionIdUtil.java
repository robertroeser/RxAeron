package io.reactivex.aeron;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/4/15.
 */
public class TransactionIdUtil {
    private static AtomicLong id = new AtomicLong(0);

    public static long getTransactionId() {
        return id.incrementAndGet();
    }

    // djb2 hash
    public static long getConnectionId(String channel) {
        long hash = 5381;

        for (int i = 0; i < channel.length(); i++) {
            hash = ((hash << 5) + hash) + channel.charAt(i);
        }

        return hash;
    }
}