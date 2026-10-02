package ru.refiq.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.refiq.batch.BatchAccumulator;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.batch.IngestRecord;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.strategy.IngestStrategy;
import ru.refiq.support.TestIngest;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.expectThrows;

public class IngestSessionTest {

    private IngestStrategy strategy;
    private OffsetCommitter committer;
    private DeadLetterPublisher deadLetters;

    @BeforeMethod
    public void setUp() {
        strategy = mock(IngestStrategy.class);
        committer = mock(OffsetCommitter.class);
        deadLetters = mock(DeadLetterPublisher.class);
    }

    @Test
    public void commitsOnlyAfterSuccessfulPersistence() {
        IngestSession session = session(1, 10_000);
        List<String> order = new ArrayList<>();
        doAnswer(invocation -> {
            order.add("process");
            return null;
        }).when(strategy).process(any());
        doAnswer(invocation -> {
            order.add("commit");
            return null;
        }).when(committer).commit(any());

        session.offer(List.of(TestIngest.record(0, 41, 10)), 0L);

        assertThat(order, contains("process", "commit"));
    }

    @Test
    public void storageFailureDoesNotCommit() {
        IngestSession session = session(1, 10_000);
        doThrow(new RetryableIngestException("clickhouse down")).when(strategy).process(any());

        expectThrows(RetryableIngestException.class, () -> session.offer(List.of(TestIngest.record(0, 1, 10)), 0L));

        verify(committer, never()).commit(any());
        assertThat(session.hasPending(), is(true));
    }

    @Test
    public void persistenceSuccessCommitsNextOffset() {
        IngestSession session = session(2, 10_000);

        session.offer(List.of(TestIngest.record(0, 5, 10), TestIngest.record(1, 8, 10)), 0L);

        Map<TopicPartition, OffsetAndMetadata> offsets = committedOffsets();
        assertThat(offsets.get(new TopicPartition("clickstream-events", 0)).offset(), is(6L));
        assertThat(offsets.get(new TopicPartition("clickstream-events", 1)).offset(), is(9L));
        assertThat(session.hasPending(), is(false));
    }

    @Test
    public void commitFailureDoesNotPersistAgainAndRetryFinishesBatch() {
        IngestSession session = session(1, 10_000);
        doThrow(new RetryableIngestException("commit failed"))
                .doNothing()
                .when(committer).commit(any());

        expectThrows(RetryableIngestException.class, () -> session.offer(List.of(TestIngest.record(0, 3, 10)), 0L));
        verify(strategy, times(1)).process(any());

        session.retry(10_000_000L);

        verify(strategy, times(1)).process(any());
        verify(committer, times(2)).commit(any());
        assertThat(session.hasPending(), is(false));
    }

    @Test
    public void idlePollFlushesBufferedRecordsOnTimeout() {
        IngestSession session = session(100, 10_000, Duration.ofMillis(500));
        List<IngestRecord> records = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            records.add(TestIngest.record(0, i, 10));
        }
        session.offer(records, 0L);
        verify(strategy, never()).process(any());

        session.onIdle(100_000_000L);
        verify(strategy, never()).process(any());

        session.onIdle(500_000_000L);

        verify(strategy, times(1)).process(any());
        verify(committer, times(1)).commit(any());
    }

    @Test
    public void retryableStorageFailureIsRetried() {
        IngestSession session = session(1, 10_000);
        doThrow(new RetryableIngestException("timeout"))
                .doNothing()
                .when(strategy).process(any());

        expectThrows(RetryableIngestException.class, () -> session.offer(List.of(TestIngest.record(0, 1, 10)), 0L));
        session.retry(10_000_000L);

        verify(strategy, times(2)).process(any());
        verify(committer, times(1)).commit(any());
    }

    @Test
    public void fatalExceptionStopsProcessing() {
        IngestSession session = session(1, 10_000);
        doThrow(new FatalIngestException("schema contract")).when(strategy).process(any());

        expectThrows(FatalIngestException.class, () -> session.offer(List.of(TestIngest.record(0, 1, 10)), 0L));
        expectThrows(FatalIngestException.class, () -> session.offer(List.of(TestIngest.record(0, 2, 10)), 1L));

        verify(committer, never()).commit(any());
        assertThat(session.isFatal(), is(true));
    }

    @Test
    public void shutdownFlushesAccumulator() {
        IngestSession session = session(100, 10_000);
        session.offer(List.of(TestIngest.record(0, 4, 10)), 0L);

        session.shutdown(1L);

        verify(strategy).process(org.mockito.ArgumentMatchers.argThat(batch ->
                batch.reason() == FlushReason.SHUTDOWN && batch.size() == 1));
        verify(committer).commit(any());
    }

    @Test
    public void failedShutdownFlushDoesNotCommit() {
        IngestSession session = session(100, 10_000);
        doThrow(new RetryableIngestException("clickhouse down")).when(strategy).process(any());
        session.offer(List.of(TestIngest.record(0, 4, 10)), 0L);

        session.shutdown(1L);

        verify(committer, never()).commit(any());
    }

    @Test
    public void rebalanceFlushesBufferedRecords() {
        IngestSession session = session(100, 10_000);
        session.offer(List.of(TestIngest.record(0, 7, 10)), 0L);

        session.onRevoke(Set.of(new TopicPartition("clickstream-events", 0)), 1L);

        verify(strategy).process(org.mockito.ArgumentMatchers.argThat(batch ->
                batch.reason() == FlushReason.REBALANCE
                        && batch.size() == 1
                        && batch.records().getFirst().offset() == 7L));
        verify(committer).commit(any());
    }

    @Test
    public void failedRebalanceFlushLeavesOffsetsUncommitted() {
        IngestSession session = session(100, 10_000);
        doThrow(new RetryableIngestException("clickhouse down")).when(strategy).process(any());
        session.offer(List.of(TestIngest.record(0, 7, 10)), 0L);

        session.onRevoke(Set.of(new TopicPartition("clickstream-events", 0)), 1L);

        verify(strategy).process(any());
        verify(committer, never()).commit(any());
    }

    @Test
    public void oversizedRecordIsIsolatedAndDoesNotBlockLaterRecords() {
        IngestSession session = session(100, 50);
        session.offer(List.of(TestIngest.record(0, 1, 1_000), TestIngest.record(0, 2, 10)), 0L);

        verify(deadLetters).publish(any());
        verify(strategy, never()).process(any());
        session.shutdown(1L);
        verify(strategy).process(org.mockito.ArgumentMatchers.argThat(batch -> batch.size() == 1));
    }

    @SuppressWarnings("unchecked")
    private Map<TopicPartition, OffsetAndMetadata> committedOffsets() {
        org.mockito.ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor =
                org.mockito.ArgumentCaptor.forClass(Map.class);
        verify(committer).commit(captor.capture());
        return captor.getValue();
    }

    private IngestSession session(int maxRecords, long maxBytes) {
        return session(maxRecords, maxBytes, Duration.ofHours(1));
    }

    private IngestSession session(int maxRecords, long maxBytes, Duration maxWait) {
        doNothing().when(strategy).process(any());
        return TestIngest.session(
                new BatchAccumulator(maxRecords, maxBytes, maxWait),
                strategy,
                committer,
                deadLetters
        );
    }
}
