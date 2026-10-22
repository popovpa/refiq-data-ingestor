package ru.refiq.strategy.clickstream;

import org.testng.annotations.Test;
import ru.refiq.batch.FlushReason;
import ru.refiq.batch.IngestBatch;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamAttribution;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamEnrichmentPipeline;
import ru.refiq.strategy.clickstream.enrichment.NoOpClickstreamAttributionResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpGeoEnrichmentService;
import ru.refiq.strategy.clickstream.enrichment.NoOpIpAddressResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpNetworkEnrichmentService;
import ru.refiq.strategy.clickstream.enrichment.TrustedClickstreamContext;
import ru.refiq.support.TestIngest;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ClickstreamTrustedEntityTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void deserializesCanonicalSnakeCaseEntityFields() {
        TrustedClickstreamContext context = CanonicalKafkaEntityContext.from(objectMapper.readTree("""
                {
                  "site_id": 123,
                  "business_id": 45,
                  "partner_id": 3000,
                  "offer_id": 2000,
                  "campaign_id": 4000,
                  "tracking_link_id": 1000,
                  "traffic_ownership": "partner"
                }
                """), null);

        assertThat(context.siteId(), is(123L));
        assertThat(context.businessId(), is(45L));
        assertThat(context.partnerId(), is(3000L));
        assertThat(context.offerId(), is(2000L));
        assertThat(context.campaignId(), is(4000L));
        assertThat(context.trackingLinkId(), is(1000L));
        assertThat(context.trafficOwnership(), is("partner"));
    }

    @Test
    public void partnerTrafficIsStoredFromKafka() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "event_id": 1,
                  "client_event_id": 100,
                  "site_key": "fpxkp",
                  "site_id": 123,
                  "business_id": 45,
                  "rqcid": "qvct4hxpuim9",
                  "tracking_link_id": 1000,
                  "offer_id": 2000,
                  "partner_id": 3000,
                  "campaign_id": 4000,
                  "traffic_ownership": "partner",
                  "event_type": "click"
                }
                """);

        assertThat(row.siteKey(), is("fpxkp"));
        assertThat(row.siteId(), is(123L));
        assertThat(row.businessId(), is(45L));
        assertThat(row.rqcid(), is("qvct4hxpuim9"));
        assertThat(row.trackingLinkId(), is(1000L));
        assertThat(row.offerId(), is(2000L));
        assertThat(row.partnerId(), is(3000L));
        assertThat(row.campaignId(), is(4000L));
        assertThat(row.trafficOwnership(), is("partner"));
    }

    @Test
    public void businessOwnedTrafficKeepsZeroPartner() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "site_key": "fpxkp",
                  "site_id": 123,
                  "business_id": 45,
                  "tracking_link_id": 1001,
                  "offer_id": 2001,
                  "partner_id": 0,
                  "campaign_id": 4001,
                  "traffic_ownership": "business",
                  "event_type": "page_view"
                }
                """);

        assertThat(row.siteId(), is(123L));
        assertThat(row.businessId(), is(45L));
        assertThat(row.trackingLinkId(), is(1001L));
        assertThat(row.offerId(), is(2001L));
        assertThat(row.partnerId(), is(0L));
        assertThat(row.campaignId(), is(4001L));
        assertThat(row.trafficOwnership(), is("business"));
    }

    @Test
    public void missingAttributionIsStoredAsDefaults() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "site_key": "fpxkp",
                  "site_id": 123,
                  "business_id": 45,
                  "rqcid": "",
                  "tracking_link_id": 0,
                  "offer_id": 0,
                  "partner_id": 0,
                  "campaign_id": 0,
                  "traffic_ownership": "",
                  "event_type": "page_view"
                }
                """);

        assertThat(row.siteId(), is(123L));
        assertThat(row.businessId(), is(45L));
        assertThat(row.rqcid(), is(""));
        assertThat(row.trackingLinkId(), is(0L));
        assertThat(row.offerId(), is(0L));
        assertThat(row.partnerId(), is(0L));
        assertThat(row.campaignId(), is(0L));
        assertThat(row.trafficOwnership(), is(""));
    }

    @Test
    public void unknownRqcidIsKeptWithoutLookup() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "site_key": "fpxkp",
                  "site_id": 123,
                  "business_id": 45,
                  "rqcid": "unknown123456",
                  "tracking_link_id": 0,
                  "offer_id": 0,
                  "partner_id": 0,
                  "campaign_id": 0,
                  "traffic_ownership": "",
                  "event_type": "page_view"
                }
                """);

        assertThat(row.rqcid(), is("unknown123456"));
        assertThat(row.siteId(), is(123L));
        assertThat(row.businessId(), is(45L));
        assertThat(row.offerId(), is(0L));
        assertThat(row.partnerId(), is(0L));
    }

    @Test
    public void legacyEventWithoutEntityFieldsUsesDefaults() {
        ClickstreamEventRecord row = ingest("""
                {"schemaVersion":1,"clientEventId":"abc123abc123abc123ab","type":"page_view","occurredAtMs":1710000000000,"script_id":"3jty6","payload":{"attribution":{"rqcid":"abc123xyz789"}}}
                """);

        assertThat(row.siteKey(), is("3jty6"));
        assertThat(row.rqcid(), is("abc123xyz789"));
        assertThat(row.siteId(), is(0L));
        assertThat(row.businessId(), is(0L));
        assertThat(row.partnerId(), is(0L));
        assertThat(row.offerId(), is(0L));
        assertThat(row.campaignId(), is(0L));
        assertThat(row.trackingLinkId(), is(0L));
        assertThat(row.trafficOwnership(), is(""));
    }

    @Test
    public void trustedKafkaValuesSurviveEnrichment() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "site_key": "fpxkp",
                  "site_id": 101,
                  "business_id": 202,
                  "partner_id": 303,
                  "offer_id": 404,
                  "campaign_id": 505,
                  "tracking_link_id": 606,
                  "traffic_ownership": "partner",
                  "event_type": "click",
                  "utm_source": "ads",
                  "page_pathname": "/pricing"
                }
                """);

        assertThat(row.siteId(), is(101L));
        assertThat(row.businessId(), is(202L));
        assertThat(row.partnerId(), is(303L));
        assertThat(row.offerId(), is(404L));
        assertThat(row.campaignId(), is(505L));
        assertThat(row.trackingLinkId(), is(606L));
        assertThat(row.trafficOwnership(), is("partner"));
        assertThat(row.utmSource(), is("ads"));
        assertThat(row.pagePathname(), is("/pricing"));
    }

    @Test
    public void noopResolverDoesNotReplaceTrustedValuesWithZeros() {
        IngestMetrics metrics = TestIngest.metrics();
        TrustedClickstreamContext trusted = new TrustedClickstreamContext(
                null, null, null, null, null, null, null, null, null,
                101L, 202L, 303L, 404L, 505L, 606L, "partner"
        );
        ClickstreamAttribution resolved = new NoOpClickstreamAttributionResolver(metrics).resolve(trusted);

        assertThat(resolved.siteId(), is(101L));
        assertThat(resolved.businessId(), is(202L));
        assertThat(resolved.partnerId(), is(303L));
        assertThat(resolved.offerId(), is(404L));
        assertThat(resolved.campaignId(), is(505L));
        assertThat(resolved.trackingLinkId(), is(606L));
        assertThat(resolved.trafficOwnership(), is("partner"));

        ClickstreamEventRecord record = new ClickstreamEventRecord();
        pipeline().enrich(trusted, record);
        assertThat(record.siteId(), is(101L));
        assertThat(record.trafficOwnership(), is("partner"));
    }

    @Test
    public void nestedBrowserIdsAreNotTrusted() {
        ClickstreamEventRecord row = ingest("""
                {
                  "schemaVersion": 1,
                  "site_key": "fpxkp",
                  "event_type": "page_view",
                  "payload": {"site_id": 999, "business_id": 888, "traffic_ownership": "partner"}
                }
                """);

        assertThat(row.siteId(), is(0L));
        assertThat(row.businessId(), is(0L));
        assertThat(row.trafficOwnership(), is(""));
    }

    @Test
    public void negativeEntityIdGoesToTheDeadLetter() {
        DeadLetterPublisher deadLetters = mock(DeadLetterPublisher.class);
        ClickstreamWriter writer = mock(ClickstreamWriter.class);
        strategy(writer, deadLetters).process(batch("""
                {"schemaVersion":1,"site_key":"fpxkp","site_id":-1,"event_type":"click"}
                """));

        verify(writer, never()).write(any());
        verify(deadLetters).publish(any());
    }

    @Test
    public void unknownTrafficOwnershipGoesToTheDeadLetter() {
        DeadLetterPublisher deadLetters = mock(DeadLetterPublisher.class);
        ClickstreamWriter writer = mock(ClickstreamWriter.class);
        strategy(writer, deadLetters).process(batch("""
                {"schemaVersion":1,"site_key":"fpxkp","traffic_ownership":"affiliate","event_type":"click"}
                """));

        verify(writer, never()).write(any());
        verify(deadLetters).publish(any());
    }

    @Test
    public void entityColumnsStayInInsertOrder() {
        List<String> names = ClickstreamColumn.names();
        assertThat(
                List.of(
                        names.get(names.indexOf("site_id")),
                        names.get(names.indexOf("business_id")),
                        names.get(names.indexOf("partner_id")),
                        names.get(names.indexOf("offer_id")),
                        names.get(names.indexOf("campaign_id")),
                        names.get(names.indexOf("tracking_link_id")),
                        names.get(names.indexOf("traffic_ownership"))
                ),
                contains("site_id", "business_id", "partner_id", "offer_id", "campaign_id", "tracking_link_id", "traffic_ownership")
        );
        assertThat(names.indexOf("site_id") < names.indexOf("business_id"), is(true));
        assertThat(names.indexOf("tracking_link_id") + 1, is(names.indexOf("traffic_ownership")));
    }

    private ClickstreamEventRecord ingest(String json) {
        ClickstreamWriter writer = mock(ClickstreamWriter.class);
        strategy(writer, mock(DeadLetterPublisher.class)).process(batch(json));
        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(writer).write(captor.capture());
        return (ClickstreamEventRecord) captor.getValue().getFirst();
    }

    private ClickstreamIngestStrategy strategy(ClickstreamWriter writer, DeadLetterPublisher deadLetters) {
        return new ClickstreamIngestStrategy(
                new ClickstreamValidator(objectMapper),
                new ClickstreamMapper(objectMapper, new ExactLongClientEventIdMapper()),
                pipeline(),
                writer,
                deadLetters,
                TestIngest.metrics()
        );
    }

    private static ClickstreamEnrichmentPipeline pipeline() {
        IngestMetrics metrics = TestIngest.metrics();
        return new ClickstreamEnrichmentPipeline(
                Clock.fixed(Instant.parse("2026-10-03T00:00:00Z"), ZoneOffset.UTC),
                () -> 42L,
                new NoOpIpAddressResolver(metrics),
                new NoOpGeoEnrichmentService(metrics),
                new NoOpNetworkEnrichmentService(metrics),
                new NoOpClickstreamAttributionResolver(metrics)
        );
    }

    private static IngestBatch batch(String json) {
        return IngestBatch.of(List.of(TestIngest.json(1, json)), FlushReason.SIZE);
    }
}
