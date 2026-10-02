package ru.refiq.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;
import ru.refiq.batch.IngestBatch;
import ru.refiq.config.DataIngestProperties;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class IngestMetrics {

    private final MeterRegistry registry;
    private final String type;
    private final Counter received;
    private final Counter processed;
    private final Counter invalid;
    private final Counter dlq;
    private final Counter storageErrors;
    private final Counter commitErrors;
    private final Counter retries;
    private final DistributionSummary batchRecords;
    private final DistributionSummary batchBytes;
    private final Timer batchProcessing;
    private final Timer storageWrite;
    private final Counter eventsReceived;
    private final Counter eventsInserted;
    private final Counter eventsInvalid;
    private final Counter eventsFailed;
    private final Counter clickhouseBatches;
    private final DistributionSummary clickhouseBatchEvents;
    private final Timer clickhouseInsert;
    private final ConcurrentMap<String, Counter> enrichmentStubs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> batches = new ConcurrentHashMap<>();

    public IngestMetrics(MeterRegistry registry, DataIngestProperties properties, BufferGauge buffer) {
        this.registry = registry;
        this.type = properties.getSource().getType();
        this.received = counter("data.ingest.records.received");
        this.processed = counter("data.ingest.records.processed");
        this.invalid = counter("data.ingest.records.invalid");
        this.dlq = counter("data.ingest.records.dlq");
        this.storageErrors = counter("data.ingest.storage.write.errors");
        this.commitErrors = counter("data.ingest.kafka.commit.errors");
        this.retries = counter("data.ingest.retries");
        this.batchRecords = DistributionSummary.builder("data.ingest.batch.records")
                .tag("type", type)
                .register(registry);
        this.batchBytes = DistributionSummary.builder("data.ingest.batch.bytes")
                .tag("type", type)
                .register(registry);
        this.batchProcessing = Timer.builder("data.ingest.batch.processing")
                .tag("type", type)
                .register(registry);
        this.storageWrite = Timer.builder("data.ingest.storage.write")
                .tag("type", type)
                .register(registry);
        this.eventsReceived = Counter.builder("received.events").tag("type", type).register(registry);
        this.eventsInserted = Counter.builder("inserted.events").tag("type", type).register(registry);
        this.eventsInvalid = Counter.builder("invalid.events").tag("type", type).register(registry);
        this.eventsFailed = Counter.builder("failed.events").tag("type", type).register(registry);
        this.clickhouseBatches = Counter.builder("clickhouse.batches").tag("type", type).register(registry);
        this.clickhouseBatchEvents = DistributionSummary.builder("clickhouse.batch.events")
                .tag("type", type)
                .register(registry);
        this.clickhouseInsert = Timer.builder("clickhouse.insert.duration")
                .tag("type", type)
                .register(registry);
        Gauge.builder("data.ingest.buffer.records", buffer, gauge -> gauge.records())
                .tag("type", type)
                .register(registry);
        Gauge.builder("data.ingest.buffer.bytes", buffer, gauge -> gauge.bytes())
                .tag("type", type)
                .register(registry);
    }

    private Counter counter(String name) {
        return Counter.builder(name).tag("type", type).register(registry);
    }

    public void received(int count) {
        received.increment(count);
    }

    public void processed(int count) {
        processed.increment(count);
    }

    public void invalid(int count) {
        invalid.increment(count);
        eventsInvalid.increment(count);
    }

    public void eventsReceived(int count) {
        eventsReceived.increment(count);
    }

    public void eventsInserted(int count) {
        eventsInserted.increment(count);
    }

    public void eventsFailed(int count) {
        eventsFailed.increment(count);
    }

    public void clickhouseBatch(int events) {
        clickhouseBatches.increment();
        clickhouseBatchEvents.record(events);
    }

    public void enrichmentStub(String component) {
        enrichmentStubs.computeIfAbsent(component, name -> Counter.builder("enrichment.stub")
                .tag("type", type)
                .tag("component", name)
                .register(registry)).increment();
    }

    public Timer.Sample startClickHouseInsert() {
        return Timer.start(registry);
    }

    public void stopClickHouseInsert(Timer.Sample sample) {
        if (sample != null) {
            sample.stop(clickhouseInsert);
        }
    }

    public void dlq(int count) {
        dlq.increment(count);
    }

    public void storageError() {
        storageErrors.increment();
    }

    public void commitError() {
        commitErrors.increment();
    }

    public void retry() {
        retries.increment();
    }

    public Timer.Sample startBatch() {
        return Timer.start(registry);
    }

    public Timer.Sample startStorageWrite() {
        return Timer.start(registry);
    }

    public void stopBatch(Timer.Sample sample) {
        if (sample != null) {
            sample.stop(batchProcessing);
        }
    }

    public void stopStorageWrite(Timer.Sample sample) {
        if (sample != null) {
            sample.stop(storageWrite);
        }
    }

    public void batchCompleted(IngestBatch batch) {
        batches.computeIfAbsent(batch.reason().label(), label -> Counter.builder("data.ingest.batches")
                .tag("type", type)
                .tag("flush_reason", label)
                .register(registry)).increment();
        batchRecords.record(batch.size());
        batchBytes.record(batch.bytes());
    }
}
