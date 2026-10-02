package ru.refiq.support;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import ru.refiq.batch.BatchAccumulator;
import ru.refiq.batch.IngestRecord;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.consumer.IngestSession;
import ru.refiq.consumer.OffsetCommitter;
import ru.refiq.consumer.RetryPolicy;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.metrics.BufferGauge;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.IngestStrategy;

import java.time.Duration;

public final class TestIngest {

    private TestIngest() {
    }

    public static IngestRecord record(int partition, long offset, long bytes) {
        int stored = (int) Math.min(bytes, 8);
        return new IngestRecord("clickstream-events", partition, offset, null, new byte[stored], bytes, 0L);
    }

    public static IngestRecord json(long offset, String payload) {
        byte[] value = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return new IngestRecord("clickstream-events", 0, offset, null, value, value.length, 0L);
    }

    public static DataIngestProperties auditProperties() {
        DataIngestProperties properties = properties();
        properties.getSource().setTopic("audit-events");
        properties.getSource().setType("audit");
        properties.getSource().setGroupId("data-ingest-audit");
        properties.getStorage().setClickhouse(null);
        DataIngestProperties.Postgres postgres = new DataIngestProperties.Postgres();
        postgres.setUrl("jdbc:postgresql://localhost:5432/refiq");
        postgres.setUsername("refiq");
        postgres.setPassword("refiq");
        postgres.setTable("audit_logs");
        postgres.setRequestTimeout(Duration.ofSeconds(30));
        properties.getStorage().setPostgres(postgres);
        return properties;
    }

    public static DataIngestProperties properties() {
        DataIngestProperties properties = new DataIngestProperties();
        properties.getSource().setTopic("clickstream-events");
        properties.getSource().setType("clickstream");
        properties.getSource().setGroupId("data-ingest-clickstream");
        properties.getKafka().setBootstrapServers("localhost:9092");
        properties.getKafka().setMaxPollInterval(Duration.ofMinutes(5));
        properties.getBatch().setMaxRecords(5000);
        properties.getBatch().setMaxBytes(5_242_880);
        properties.getBatch().setMaxWait(Duration.ofMillis(500));
        properties.getBatch().setPollTimeout(Duration.ofMillis(100));
        properties.getRetry().setInitialBackoff(Duration.ofMillis(200));
        properties.getRetry().setMaxBackoff(Duration.ofSeconds(5));
        properties.getMonitoring().setPort(9091);
        properties.getStorage().getClickhouse().setUrl("http://localhost:8123");
        properties.getStorage().getClickhouse().setDatabase("default");
        properties.getStorage().getClickhouse().setUsername("default");
        properties.getStorage().getClickhouse().setPassword("");
        properties.getStorage().getClickhouse().setTable("clickstream_events");
        properties.getStorage().getClickhouse().setRequestTimeout(Duration.ofSeconds(30));
        return properties;
    }

    public static IngestMetrics metrics() {
        return new IngestMetrics(new SimpleMeterRegistry(), properties(), new BufferGauge());
    }

    public static IngestSession session(
            BatchAccumulator accumulator,
            IngestStrategy strategy,
            OffsetCommitter committer,
            DeadLetterPublisher deadLetters
    ) {
        return new IngestSession(
                accumulator,
                strategy,
                committer,
                deadLetters,
                metrics(),
                new RetryPolicy(Duration.ofMillis(10), Duration.ofSeconds(1)),
                "clickstream",
                new BufferGauge()
        );
    }
}
