package ru.refiq.strategy.clickstream;

import tools.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamEnrichmentPipeline;
import ru.refiq.strategy.clickstream.enrichment.NoOpClickstreamAttributionResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpGeoEnrichmentService;
import ru.refiq.strategy.clickstream.enrichment.NoOpIpAddressResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpNetworkEnrichmentService;
import ru.refiq.support.TestIngest;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClickstreamIngestStrategyTest {

    @Test
    public void invalidRecordDoesNotBlockValidRecords() {
        ClickstreamWriter writer = mock(ClickstreamWriter.class);
        DeadLetterPublisher deadLetters = mock(DeadLetterPublisher.class);
        ObjectMapper objectMapper = new ObjectMapper();
        IngestMetrics metrics = TestIngest.metrics();
        Clock clock = Clock.fixed(Instant.parse("2024-01-02T03:04:05Z"), ZoneOffset.UTC);
        ClickstreamEnrichmentPipeline enrichment = new ClickstreamEnrichmentPipeline(
                clock,
                () -> 42L,
                new NoOpIpAddressResolver(metrics),
                new NoOpGeoEnrichmentService(metrics),
                new NoOpNetworkEnrichmentService(metrics),
                new NoOpClickstreamAttributionResolver(metrics)
        );
        ClickstreamIngestStrategy strategy = new ClickstreamIngestStrategy(
                new ClickstreamValidator(objectMapper),
                new ClickstreamMapper(objectMapper, new ExactLongClientEventIdMapper()),
                enrichment,
                writer,
                deadLetters,
                metrics
        );
        IngestBatch batch = IngestBatch.of(List.of(
                TestIngest.json(1, "not-json"),
                TestIngest.json(2, """
                        {"schemaVersion":1,"clientEventId":"abc123abc123abc123ab","type":"page_view","occurredAtMs":1710000000000,"script_id":"3jty6","payload":{"attribution":{"rqcid":"abc123xyz789"}}}
                        """)
        ), FlushReason.SIZE);

        strategy.process(batch);

        verify(writer).write(argThat(events -> events.size() == 1
                && events.getFirst().clientEventId() == 0L
                && events.getFirst().eventType().equals("page_view")
                && events.getFirst().siteKey().equals("3jty6")
                && events.getFirst().rqcid().equals("abc123xyz789")
                && events.getFirst().capturedTs() == clock.millis()
                && events.getFirst().eventId() == 42L
                && "::".equals(events.getFirst().ipAddress())
                && events.getFirst().siteId() == 0L));
        verify(deadLetters).publish(any());
        assertThat(strategy.type(), is("clickstream"));
    }
}
