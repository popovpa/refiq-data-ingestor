package ru.refiq.strategy.audit;

import org.testng.annotations.Test;
import ru.refiq.batch.BatchAccumulator;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.consumer.IngestSession;
import ru.refiq.consumer.OffsetCommitter;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.support.TestIngest;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.expectThrows;

public class AuditIngestStrategyTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void invalidEventGoesToTheDeadLetterTopicAndDoesNotBlockTheBatch() {
        AuditWriter writer = mock(AuditWriter.class);
        DeadLetterPublisher deadLetters = mock(DeadLetterPublisher.class);
        AuditIngestStrategy strategy = strategy(writer, deadLetters);
        IngestBatch batch = IngestBatch.of(List.of(
                TestIngest.json(1, "not-json"),
                TestIngest.json(2, validEvent("evt_ok"))
        ), FlushReason.SIZE);

        strategy.process(batch);

        verify(deadLetters).publish(any());
        verify(writer).write(org.mockito.ArgumentMatchers.argThat(events ->
                events.size() == 1 && "evt_ok".equals(events.getFirst().eventId())
        ));
    }

    @Test
    public void duplicateEventIdsAreBothHandedToTheWriter() {
        AuditWriter writer = mock(AuditWriter.class);
        AuditIngestStrategy strategy = strategy(writer, mock(DeadLetterPublisher.class));

        strategy.process(IngestBatch.of(List.of(
                TestIngest.json(1, validEvent("evt_same")),
                TestIngest.json(2, validEvent("evt_same"))
        ), FlushReason.SIZE));

        verify(writer).write(org.mockito.ArgumentMatchers.argThat(events -> events.size() == 2));
    }

    @Test
    public void temporaryPostgresFailureDoesNotCommitTheOffset() {
        AuditWriter writer = mock(AuditWriter.class);
        org.mockito.Mockito.doThrow(new RetryableIngestException("connection refused"))
                .when(writer).write(any());
        OffsetCommitter committer = mock(OffsetCommitter.class);
        IngestSession session = new IngestSession(
                new BatchAccumulator(1, 10_000, Duration.ofHours(1)),
                strategy(writer, mock(DeadLetterPublisher.class)),
                committer,
                mock(DeadLetterPublisher.class),
                TestIngest.metrics(),
                new ru.refiq.consumer.RetryPolicy(Duration.ofMillis(1), Duration.ofMillis(1)),
                "audit",
                new ru.refiq.metrics.BufferGauge()
        );

        expectThrows(RetryableIngestException.class, () -> session.offer(List.of(TestIngest.json(7, validEvent("evt_01"))), 0L));

        verify(committer, never()).commit(any());
    }

    private AuditIngestStrategy strategy(AuditWriter writer, DeadLetterPublisher deadLetters) {
        return new AuditIngestStrategy(
                new AuditEventValidator(objectMapper),
                new AuditEventMapper(objectMapper),
                writer,
                deadLetters,
                TestIngest.metrics()
        );
    }

    private static String validEvent(String eventId) {
        return """
                {"schemaVersion":1,"eventId":"%s","eventType":"OFFER_UPDATED","occurredAt":"2026-10-03T12:30:15.123Z","accountId":"acc_123","actor":{"type":"USER","id":"usr_456"},"entity":{"type":"OFFER","id":"off_789"},"action":"UPDATE","requestId":"req_1","correlationId":"corr_1","changes":{"holdPeriodDays":{"before":7,"after":14}},"before":{"holdPeriodDays":7},"after":{"holdPeriodDays":14},"metadata":{}}
                """.formatted(eventId);
    }
}
