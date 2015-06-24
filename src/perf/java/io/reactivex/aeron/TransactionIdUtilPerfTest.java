package io.reactivex.aeron;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class TransactionIdUtilPerfTest {
    private static final String STRING_TO_HASH = "aeron:udp?remote=localhost:43450";

    @Benchmark
    public void benchmarkConnectionId(Blackhole bh) throws InterruptedException {
        long connectionId = TransactionIdUtil.getConnectionId(STRING_TO_HASH);
        bh.consume(connectionId);
    }

    @Benchmark
    public void benchmarkTransactionId(Blackhole bh) throws InterruptedException {
        long transactionId = TransactionIdUtil.getTransactionId();
        bh.consume(transactionId);
    }

}