package ru.refiq.batch;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Bounded buffer of inbound Kafka records. Flush when records, bytes, or age trip.
 * The age clock starts when the first record enters an empty accumulator and uses the
 * caller-supplied monotonic timestamp.
 */
public final class BatchAccumulator {

    private final int maxRecords;
    private final long maxBytes;
    private final long maxWaitNanos;
    private final List<IngestRecord> records = new ArrayList<>();
    private long bytes;
    private long firstRecordNanos = Long.MIN_VALUE;

    public BatchAccumulator(int maxRecords, long maxBytes, Duration maxWait) {
        if (maxRecords <= 0 || maxBytes <= 0 || maxWait == null || maxWait.isZero() || maxWait.isNegative()) {
            throw new IllegalArgumentException("batch limits must be > 0");
        }
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxWaitNanos = maxWait.toNanos();
    }

    public AddDecision add(IngestRecord record, long nowNanos) {
        long recordBytes = record.estimatedBytes();
        if (recordBytes > maxBytes) {
            return AddDecision.oversized();
        }
        if (!records.isEmpty() && bytes + recordBytes > maxBytes) {
            return AddDecision.flushBeforeAdd(FlushReason.BYTES);
        }
        records.add(record);
        bytes += recordBytes;
        if (records.size() == 1) {
            firstRecordNanos = nowNanos;
        }
        if (records.size() >= maxRecords) {
            return AddDecision.acceptedFlush(FlushReason.SIZE);
        }
        if (bytes >= maxBytes) {
            return AddDecision.acceptedFlush(FlushReason.BYTES);
        }
        return AddDecision.accepted();
    }

    public boolean isTimeToFlush(long nowNanos) {
        return !records.isEmpty() && nowNanos - firstRecordNanos >= maxWaitNanos;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }

    public long bytes() {
        return bytes;
    }

    public IngestBatch drain(FlushReason reason) {
        IngestBatch batch = IngestBatch.of(records, reason);
        records.clear();
        bytes = 0;
        firstRecordNanos = Long.MIN_VALUE;
        return batch;
    }

    public IngestBatch extract(Set<TopicPartition> partitions, FlushReason reason) {
        List<IngestRecord> taken = new ArrayList<>();
        List<IngestRecord> kept = new ArrayList<>();
        for (IngestRecord record : records) {
            if (partitions.contains(record.topicPartition())) {
                taken.add(record);
            } else {
                kept.add(record);
            }
        }
        records.clear();
        records.addAll(kept);
        recompute();
        return IngestBatch.of(taken, reason);
    }

    private void recompute() {
        bytes = 0;
        firstRecordNanos = Long.MIN_VALUE;
        for (IngestRecord record : records) {
            bytes += record.estimatedBytes();
            if (firstRecordNanos == Long.MIN_VALUE || record.enqueuedAtNanos() < firstRecordNanos) {
                firstRecordNanos = record.enqueuedAtNanos();
            }
        }
    }
}
