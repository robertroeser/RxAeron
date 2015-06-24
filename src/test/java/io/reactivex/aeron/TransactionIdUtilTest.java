package io.reactivex.aeron;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Set;

public class TransactionIdUtilTest {
    @Test
    public void testHashesToSameValue() {
        String s1 = "Please hash me";
        String s2 = "Please hash me";

        long hash1 = TransactionIdUtil.getConnectionId(s1);
        long hash2 = TransactionIdUtil.getConnectionId(s2);

        Assert.assertEquals(hash1, hash2);
    }

    @Test
    public void testHashesToDifferentValue() {
        String s1 = "Please hash me";
        String s2 = "Please hash me as well";

        long hash1 = TransactionIdUtil.getConnectionId(s1);
        long hash2 = TransactionIdUtil.getConnectionId(s2);

        Assert.assertFalse(hash1 == hash2);
    }

    @Test
    public void testGetTransactionId() {
        int count = 0;

        Set<Long> set = Sets.newHashSet();

        for (int i = 0; i < 1_000_000; i++) {
            long transactionId = TransactionIdUtil.getTransactionId();
            if (i % 1_000 == 0) {
                count++;
                set.add(transactionId);
            }

        }

        Assert.assertEquals(count, set.size());
    }

}