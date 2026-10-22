package ru.refiq.strategy.audit;

import org.springframework.stereotype.Component;
import ru.refiq.batch.IngestBatch;
import ru.refiq.batch.IngestRecord;
import ru.refiq.config.ConditionalOnAuditPipeline;
import ru.refiq.error.DeadLetter;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.InvalidRecordException;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.IngestStrategy;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnAuditPipeline
public class AuditIngestStrategy implements IngestStrategy {

    private final AuditEventValidator validator;
    private final AuditEventMapper mapper;
    private final AuditWriter writer;
    private final DeadLetterPublisher deadLetterPublisher;
    private final IngestMetrics metrics;

    public AuditIngestStrategy(
            AuditEventValidator validator,
            AuditEventMapper mapper,
            AuditWriter writer,
            DeadLetterPublisher deadLetterPublisher,
            IngestMetrics metrics
    ) {
        this.validator = validator;
        this.mapper = mapper;
        this.writer = writer;
        this.deadLetterPublisher = deadLetterPublisher;
        this.metrics = metrics;
    }

    @Override
    public String type() {
        return "audit";
    }

    @Override
    public void process(IngestBatch batch) {
        List<AuditEvent> valid = new ArrayList<>();
        List<DeadLetter> invalid = new ArrayList<>();
        for (IngestRecord record : batch.records()) {
            collect(record, valid, invalid);
        }
        if (!valid.isEmpty()) {
            writer.write(valid);
            metrics.processed(valid.size());
        }
        for (DeadLetter letter : invalid) {
            deadLetterPublisher.publish(letter);
            metrics.dlq(1);
        }
    }

    private void collect(IngestRecord record, List<AuditEvent> valid, List<DeadLetter> invalid) {
        metrics.eventsReceived(1);
        try {
            JsonNode node = validator.parse(record.value());
            valid.add(mapper.map(node));
        } catch (InvalidRecordException e) {
            invalid.add(DeadLetter.invalid(record, type(), e.getMessage()));
            metrics.invalid(1);
        } catch (RuntimeException e) {
            throw new FatalIngestException("audit mapping failed", e);
        }
    }
}
