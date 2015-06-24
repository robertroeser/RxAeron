package io.reactivex.aeron;


import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/4/15.
 */
public class TransactionIdUtil {

    private static final HashFunction function = Hashing.murmur3_128();

    private static AtomicLong counter = new AtomicLong(0);

    public static long getTransactionId() {
        return counter.getAndIncrement();
    }

    /**
     * Takes a channel id and uses murmur3 128-bit hashcode
     *
     * @param channel the channel to hash
     * @return a long representing the connection id
     *
    */
    public static long getConnectionId(String channel) {
        HashCode hashCode = function.hashBytes(channel.getBytes());
        return  hashCode.asLong();
    }

}