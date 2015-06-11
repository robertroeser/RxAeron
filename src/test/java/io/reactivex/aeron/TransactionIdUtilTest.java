package io.reactivex.aeron;

import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

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
}