package ru.refiq.batch;

import org.testng.annotations.Test;
import ru.refiq.support.TestIngest;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class BatchAccumulatorTest {

    @Test
    public void flushesWhenMaxRecordsReached() {
        BatchAccumulator accumulator = accumulator(3, 10_000, Duration.ofSeconds(10));

        assertThat(accumulator.add(TestIngest.record(0, 1, 10), 0).type(), is(AddDecision.Type.ACCEPTED));
        assertThat(accumulator.add(TestIngest.record(0, 2, 10), 1).type(), is(AddDecision.Type.ACCEPTED));
        AddDecision third = accumulator.add(TestIngest.record(0, 3, 10), 2);

        assertThat(third.type(), is(AddDecision.Type.ACCEPTED_FLUSH));
        assertThat(third.reason(), is(FlushReason.SIZE));
        assertThat(accumulator.size(), is(3));
    }

    @Test
    public void flushesWhenMaxBytesReached() {
        BatchAccumulator accumulator = accumulator(100, 100, Duration.ofSeconds(10));

        assertThat(accumulator.add(TestIngest.record(0, 1, 60), 0).type(), is(AddDecision.Type.ACCEPTED));
        AddDecision second = accumulator.add(TestIngest.record(0, 2, 40), 1);

        assertThat(second.type(), is(AddDecision.Type.ACCEPTED_FLUSH));
        assertThat(second.reason(), is(FlushReason.BYTES));
        assertThat(accumulator.bytes(), is(100L));
    }

    @Test
    public void flushesExistingBatchBeforeOversizedAddition() {
        BatchAccumulator accumulator = accumulator(100, 100, Duration.ofSeconds(10));
        accumulator.add(TestIngest.record(0, 1, 80), 0);

        AddDecision decision = accumulator.add(TestIngest.record(0, 2, 30), 1);

        assertThat(decision.type(), is(AddDecision.Type.FLUSH_BEFORE_ADD));
        assertThat(decision.reason(), is(FlushReason.BYTES));
        assertThat(accumulator.size(), is(1));
        assertThat(accumulator.bytes(), is(80L));
    }

    @Test
    public void flushesWhenMaxWaitElapses() {
        BatchAccumulator accumulator = accumulator(100, 10_000, Duration.ofMillis(500));
        accumulator.add(TestIngest.record(0, 1, 10), 0);

        assertThat(accumulator.isTimeToFlush(499_000_000L), is(false));
        assertThat(accumulator.isTimeToFlush(500_000_000L), is(true));
    }

    @Test
    public void timeoutTripsWithoutFurtherRecords() {
        BatchAccumulator accumulator = accumulator(100, 10_000, Duration.ofMillis(500));
        accumulator.add(TestIngest.record(0, 1, 10), 0);

        assertThat(accumulator.isTimeToFlush(100_000_000L), is(false));
        assertThat(accumulator.isTimeToFlush(200_000_000L), is(false));
        assertThat(accumulator.isTimeToFlush(500_000_000L), is(true));
        assertThat(accumulator.drain(FlushReason.TIMEOUT).size(), is(1));
    }

    @Test
    public void clearsAfterSuccessfulDrain() {
        BatchAccumulator accumulator = accumulator(2, 10_000, Duration.ofSeconds(10));
        accumulator.add(TestIngest.record(0, 1, 10), 0);
        accumulator.add(TestIngest.record(0, 2, 10), 1);

        IngestBatch batch = accumulator.drain(FlushReason.SIZE);

        assertThat(batch.size(), is(2));
        assertThat(accumulator.isEmpty(), is(true));
        assertThat(accumulator.bytes(), is(0L));
        assertThat(accumulator.isTimeToFlush(Long.MAX_VALUE), is(false));
        assertThat(accumulator.add(TestIngest.record(0, 3, 10), 5).reason(), is(nullValue()));
    }

    @Test
    public void oversizedSingleRecordDoesNotBlockAccumulator() {
        BatchAccumulator accumulator = accumulator(100, 100, Duration.ofSeconds(10));
        accumulator.add(TestIngest.record(0, 1, 40), 0);

        AddDecision oversized = accumulator.add(TestIngest.record(0, 2, 101), 1);

        assertThat(oversized.type(), is(AddDecision.Type.OVERSIZED));
        assertThat(accumulator.size(), is(1));
        assertThat(accumulator.add(TestIngest.record(0, 3, 40), 2).type(), is(AddDecision.Type.ACCEPTED));
        assertThat(accumulator.size(), is(2));
    }

    private static BatchAccumulator accumulator(int maxRecords, long maxBytes, Duration maxWait) {
        return new BatchAccumulator(maxRecords, maxBytes, maxWait);
    }
}
