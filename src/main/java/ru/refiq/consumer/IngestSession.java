package ru.refiq.consumer;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.refiq.batch.AddDecision;
import ru.refiq.batch.BatchAccumulator;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.batch.IngestRecord;
import ru.refiq.error.CommitAbandonedException;
import ru.refiq.error.DeadLetter;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.metrics.BufferGauge;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.IngestStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Single-threaded batch state machine. A batch that has been persisted is only
 * committed afterwards; a failed commit retries the commit and does not write storage again.
 */
public final class IngestSession {

    private static final Logger log = LoggerFactory.getLogger(IngestSession.class);

    private final BatchAccumulator accumulator;
    private final IngestStrategy strategy;
    private final OffsetCommitter offsetCommitter;
    private final DeadLetterPublisher deadLetterPublisher;
    private final IngestMetrics metrics;
    private final RetryPolicy retryPolicy;
    private final String ingestType;
    private final BufferGauge buffer;

    private Pending pending;
    private boolean wasConsumed;
    private int resumeFrom;
    private boolean fatal;

    public IngestSession(
            BatchAccumulator accumulator,
            IngestStrategy strategy,
            OffsetCommitter offsetCommitter,
            DeadLetterPublisher deadLetterPublisher,
            IngestMetrics metrics,
            RetryPolicy retryPolicy,
            String ingestType,
            BufferGauge buffer
    ) {
        this.accumulator = accumulator;
        this.strategy = strategy;
        this.offsetCommitter = offsetCommitter;
        this.deadLetterPublisher = deadLetterPublisher;
        this.metrics = metrics;
        this.retryPolicy = retryPolicy;
        this.ingestType = ingestType;
        this.buffer = buffer;
    }

    public int offer(List<IngestRecord> records, long nowNanos) {
        ensureRunning();
        if (pending != null) {
            throw new IllegalStateException("cannot accept records while a batch is awaiting retry");
        }
        int index = 0;
        try {
            if (accumulator.isTimeToFlush(nowNanos)) {
                wasConsumed = false;
                flush(FlushReason.TIMEOUT, nowNanos);
            }
            for (; index < records.size(); index++) {
                wasConsumed = false;
                accept(records.get(index), nowNanos);
                if (!wasConsumed) {
                    resumeFrom = index;
                    return index;
                }
            }
            resumeFrom = records.size();
            return records.size();
        } catch (RetryableIngestException e) {
            resumeFrom = wasConsumed ? index + 1 : index;
            throw e;
        }
    }

    public void onIdle(long nowNanos) {
        ensureRunning();
        if (pending != null || !accumulator.isTimeToFlush(nowNanos)) {
            return;
        }
        wasConsumed = false;
        flush(FlushReason.TIMEOUT, nowNanos);
    }

    public void retry(long nowNanos) {
        ensureRunning();
        if (pending == null || nowNanos < pending.nextRetryNanos) {
            return;
        }
        flushPending(nowNanos);
    }

    public void shutdown(long nowNanos) {
        if (fatal) {
            log.warn("skipping shutdown flush after fatal ingest error type={}", ingestType);
            return;
        }
        log.info("data-ingest shutdown flush type={}", ingestType);
        try {
            if (pending != null) {
                flushPending(nowNanos);
            }
            if (pending == null && !accumulator.isEmpty()) {
                flush(FlushReason.SHUTDOWN, nowNanos);
            }
        } catch (RetryableIngestException e) {
            log.error("shutdown flush failed, kafka offsets left uncommitted type={}", ingestType);
        }
    }

    public void onRevoke(Set<TopicPartition> revoked, long nowNanos) {
        if (pending != null && pending.persisted) {
            if (tryCommit(pending.batch)) {
                pending = null;
            } else {
                IngestBatch owned = pending.batch.except(revoked);
                pending = owned.isEmpty() ? null : pending.withBatch(owned);
            }
        }

        List<IngestRecord> revokedRecords = new ArrayList<>();
        if (pending != null && !pending.persisted) {
            revokedRecords.addAll(pending.batch.only(revoked).records());
            IngestBatch owned = pending.batch.except(revoked);
            pending = owned.isEmpty() ? null : pending.withBatch(owned);
        }
        revokedRecords.addAll(accumulator.extract(revoked, FlushReason.REBALANCE).records());
        updateBuffer();
        if (revokedRecords.isEmpty()) {
            return;
        }
        IngestBatch batch = IngestBatch.of(revokedRecords, FlushReason.REBALANCE);
        try {
            strategy.process(batch);
            offsetCommitter.commit(batch.commitOffsets());
            metrics.batchCompleted(batch);
            log.info(
                    "rebalance flush type={} records={} bytes={}",
                    ingestType,
                    batch.size(),
                    batch.bytes()
            );
        } catch (FatalIngestException e) {
            fatal = true;
            throw e;
        } catch (RuntimeException e) {
            log.warn(
                    "rebalance flush failed, offsets left uncommitted type={} records={}",
                    ingestType,
                    batch.size()
            );
        }
    }

    public boolean hasPending() {
        return pending != null;
    }

    public boolean retryDue(long nowNanos) {
        return pending != null && nowNanos >= pending.nextRetryNanos;
    }

    public int resumeFrom() {
        return resumeFrom;
    }

    public boolean isFatal() {
        return fatal;
    }

    private void accept(IngestRecord record, long nowNanos) {
        AddDecision decision = accumulator.add(record, nowNanos);
        switch (decision.type()) {
            case OVERSIZED -> {
                if (!accumulator.isEmpty()) {
                    flush(FlushReason.BYTES, nowNanos);
                }
                beginOversized(record, nowNanos);
            }
            case FLUSH_BEFORE_ADD -> {
                flush(decision.reason(), nowNanos);
                AddDecision added = accumulator.add(record, nowNanos);
                wasConsumed = true;
                metrics.received(1);
                updateBuffer();
                if (added.type() == AddDecision.Type.ACCEPTED_FLUSH) {
                    flush(added.reason(), nowNanos);
                }
            }
            case ACCEPTED, ACCEPTED_FLUSH -> {
                wasConsumed = true;
                metrics.received(1);
                updateBuffer();
                if (decision.type() == AddDecision.Type.ACCEPTED_FLUSH) {
                    flush(decision.reason(), nowNanos);
                }
            }
            default -> throw new IllegalStateException("unexpected add decision " + decision.type());
        }
    }

    private void beginOversized(IngestRecord record, long nowNanos) {
        pending = Pending.fresh(IngestBatch.of(List.of(record), FlushReason.BYTES), true);
        wasConsumed = true;
        metrics.received(1);
        updateBuffer();
        flushPending(nowNanos);
    }

    private void flush(FlushReason reason, long nowNanos) {
        if (accumulator.isEmpty()) {
            return;
        }
        if (pending != null) {
            throw new IllegalStateException("flush requested while a batch is pending");
        }
        pending = Pending.fresh(accumulator.drain(reason), false);
        updateBuffer();
        flushPending(nowNanos);
    }

    private void flushPending(long nowNanos) {
        var sample = metrics.startBatch();
        try {
            if (!pending.persisted) {
                if (pending.oversized) {
                    IngestRecord record = pending.batch.records().getFirst();
                    deadLetterPublisher.publish(DeadLetter.invalid(record, ingestType, "record exceeds max-bytes"));
                    metrics.invalid(1);
                    metrics.dlq(1);
                } else {
                    strategy.process(pending.batch);
                }
                pending.persisted = true;
            }
            offsetCommitter.commit(pending.batch.commitOffsets());
            metrics.batchCompleted(pending.batch);
            log.info(
                    "batch flushed type={} reason={} records={} bytes={}",
                    ingestType,
                    pending.batch.reason().label(),
                    pending.batch.size(),
                    pending.batch.bytes()
            );
            pending = null;
            updateBuffer();
        } catch (CommitAbandonedException e) {
            metrics.commitError();
            log.warn(
                    "kafka commit abandoned, batch will be redelivered type={} records={}",
                    ingestType,
                    pending.batch.size()
            );
            pending = null;
            updateBuffer();
        } catch (RetryableIngestException e) {
            scheduleRetry(nowNanos, e);
            updateBuffer();
            throw e;
        } catch (FatalIngestException e) {
            fatal = true;
            throw e;
        } catch (RuntimeException e) {
            fatal = true;
            throw new FatalIngestException("ingest batch failed", e);
        } finally {
            metrics.stopBatch(sample);
        }
    }

    private boolean tryCommit(IngestBatch batch) {
        try {
            offsetCommitter.commit(batch.commitOffsets());
            return true;
        } catch (RuntimeException e) {
            metrics.commitError();
            log.warn("kafka commit failed during rebalance type={} records={}", ingestType, batch.size());
            return false;
        }
    }

    private void scheduleRetry(long nowNanos, RetryableIngestException error) {
        pending.attempts++;
        long delayNanos = retryPolicy.delayNanos(pending.attempts);
        pending.nextRetryNanos = nowNanos + delayNanos;
        metrics.retry();
        log.warn(
                "retryable ingest failure type={} attempt={} backoffMs={} reason={}",
                ingestType,
                pending.attempts,
                TimeUnit.NANOSECONDS.toMillis(delayNanos),
                error.getMessage()
        );
    }

    private void updateBuffer() {
        int records = accumulator.size();
        long bytes = accumulator.bytes();
        if (pending != null) {
            records += pending.batch.size();
            bytes += pending.batch.bytes();
        }
        buffer.set(records, bytes);
    }

    private void ensureRunning() {
        if (fatal) {
            throw new FatalIngestException("ingest processing has been stopped");
        }
    }

    private static final class Pending {
        private IngestBatch batch;
        private boolean persisted;
        private final boolean oversized;
        private int attempts;
        private long nextRetryNanos;

        private Pending(IngestBatch batch, boolean oversized) {
            this.batch = batch;
            this.oversized = oversized;
        }

        static Pending fresh(IngestBatch batch, boolean oversized) {
            return new Pending(batch, oversized);
        }

        Pending withBatch(IngestBatch batch) {
            this.batch = batch;
            return this;
        }
    }
}
