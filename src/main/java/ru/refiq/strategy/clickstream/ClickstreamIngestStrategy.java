package ru.refiq.strategy.clickstream;

import tools.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.batch.IngestBatch;
import ru.refiq.batch.IngestRecord;
import ru.refiq.error.DeadLetter;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.InvalidRecordException;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.IngestStrategy;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamEnrichmentPipeline;

import java.util.ArrayList;
import java.util.List;

@Component
@ConditionalOnClickstreamPipeline
public class ClickstreamIngestStrategy implements IngestStrategy {

    private final ClickstreamValidator validator;
    private final ClickstreamMapper mapper;
    private final ClickstreamEnrichmentPipeline enrichment;
    private final ClickstreamWriter writer;
    private final DeadLetterPublisher deadLetterPublisher;
    private final IngestMetrics metrics;

    public ClickstreamIngestStrategy(
            ClickstreamValidator validator,
            ClickstreamMapper mapper,
            ClickstreamEnrichmentPipeline enrichment,
            ClickstreamWriter writer,
            DeadLetterPublisher deadLetterPublisher,
            IngestMetrics metrics
    ) {
        this.validator = validator;
        this.mapper = mapper;
        this.enrichment = enrichment;
        this.writer = writer;
        this.deadLetterPublisher = deadLetterPublisher;
        this.metrics = metrics;
    }

    @Override
    public String type() {
        return "clickstream";
    }

    @Override
    public void process(IngestBatch batch) {
        List<ClickstreamEventRecord> valid = new ArrayList<>();
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

    private void collect(IngestRecord record, List<ClickstreamEventRecord> valid, List<DeadLetter> invalid) {
        try {
            JsonNode node = validator.parse(record.value());
            ClickstreamMapping mapping = mapper.map(node);
            if (mapping.malformed()) {
                metrics.eventsReceived(1);
                reject(record, invalid, mapping.errors().getFirst());
                return;
            }
            metrics.eventsReceived(mapping.events().size() + mapping.errors().size());
            for (ClickstreamEventRecord event : mapping.events()) {
                enrichment.enrich(event);
                valid.add(event);
            }
            for (String error : mapping.errors()) {
                reject(record, invalid, error);
            }
        } catch (InvalidRecordException e) {
            metrics.eventsReceived(1);
            reject(record, invalid, e.getMessage());
        } catch (RuntimeException e) {
            throw new FatalIngestException("clickstream mapping failed", e);
        }
    }

    private void reject(IngestRecord record, List<DeadLetter> invalid, String description) {
        invalid.add(DeadLetter.invalid(record, type(), description));
        metrics.invalid(1);
    }
}
