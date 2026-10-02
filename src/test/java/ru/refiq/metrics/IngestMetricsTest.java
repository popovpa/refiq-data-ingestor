package ru.refiq.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.testng.annotations.Test;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.support.TestIngest;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class IngestMetricsTest {

    @Test
    public void exportsBoundedPrometheusNames() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        BufferGauge buffer = new BufferGauge();
        IngestMetrics metrics = new IngestMetrics(registry, TestIngest.properties(), buffer);
        buffer.set(3, 90);
        metrics.received(2);
        metrics.processed(2);
        metrics.invalid(1);
        metrics.dlq(1);
        metrics.storageError();
        metrics.commitError();
        metrics.retry();
        metrics.batchCompleted(IngestBatch.of(List.of(TestIngest.record(0, 1, 10)), FlushReason.TIMEOUT));
        metrics.stopBatch(metrics.startBatch());
        metrics.stopStorageWrite(metrics.startStorageWrite());
        metrics.eventsReceived(2);
        metrics.eventsInserted(2);
        metrics.eventsFailed(1);
        metrics.clickhouseBatch(2);
        metrics.enrichmentStub("ip");
        metrics.stopClickHouseInsert(metrics.startClickHouseInsert());

        String scrape = registry.scrape();

        assertThat(scrape, containsString("data_ingest_records_received_total"));
        assertThat(scrape, containsString("data_ingest_records_processed_total"));
        assertThat(scrape, containsString("data_ingest_records_invalid_total"));
        assertThat(scrape, containsString("data_ingest_records_dlq_total"));
        assertThat(scrape, containsString("data_ingest_buffer_records"));
        assertThat(scrape, containsString("data_ingest_buffer_bytes"));
        assertThat(scrape, containsString("data_ingest_batches_total"));
        assertThat(scrape, containsString("flush_reason=\"timeout\""));
        assertThat(scrape, containsString("data_ingest_batch_records"));
        assertThat(scrape, containsString("data_ingest_batch_bytes"));
        assertThat(scrape, containsString("data_ingest_batch_processing_seconds"));
        assertThat(scrape, containsString("data_ingest_storage_write_seconds"));
        assertThat(scrape, containsString("data_ingest_storage_write_errors_total"));
        assertThat(scrape, containsString("data_ingest_kafka_commit_errors_total"));
        assertThat(scrape, containsString("data_ingest_retries_total"));
        assertThat(scrape, containsString("type=\"clickstream\""));
        assertThat(scrape, containsString("received_events_total"));
        assertThat(scrape, containsString("inserted_events_total"));
        assertThat(scrape, containsString("invalid_events_total"));
        assertThat(scrape, containsString("failed_events_total"));
        assertThat(scrape, containsString("clickhouse_batches_total"));
        assertThat(scrape, containsString("clickhouse_batch_events"));
        assertThat(scrape, containsString("clickhouse_insert_duration"));
        assertThat(scrape, containsString("enrichment_stub_total"));
    }

    @Test
    public void bufferGaugeTracksAccumulatorPlusPending() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        BufferGauge buffer = new BufferGauge();
        new IngestMetrics(registry, TestIngest.properties(), buffer);
        buffer.set(4, 128);

        assertThat(registry.get("data.ingest.buffer.records").gauge().value(), is(4.0));
        assertThat(registry.get("data.ingest.buffer.bytes").gauge().value(), is(128.0));
    }
}
