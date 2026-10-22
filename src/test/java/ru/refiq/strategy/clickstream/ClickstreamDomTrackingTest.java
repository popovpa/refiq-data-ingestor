package ru.refiq.strategy.clickstream;

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
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ClickstreamDomTrackingTest {

    private static final List<ClickstreamColumn> DOM = List.of(
            ClickstreamColumn.TARGET_ELEMENT_KEY,
            ClickstreamColumn.TARGET_ELEMENT_TYPE,
            ClickstreamColumn.TARGET_ACTION_TYPE,
            ClickstreamColumn.TARGET_SEMANTIC_ROLE,
            ClickstreamColumn.TARGET_IDENTITY_SOURCE,
            ClickstreamColumn.TARGET_FORM_ID
    );

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void storesAllDomFields() {
        ClickstreamEventRecord row = map("""
                {
                  "schemaVersion": 1,
                  "event_type": "click",
                  "site_key": "fpxkp",
                  "rqcid": "qvct4hxpuim9",
                  "target_element_key": "cta-primary",
                  "target_element_type": "button",
                  "target_action_type": "submit",
                  "target_semantic_role": "primary",
                  "target_identity_source": "dom-id",
                  "target_form_id": "checkout"
                }
                """);

        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_KEY), is("cta-primary"));
        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_TYPE), is("button"));
        assertThat(text(row, ClickstreamColumn.TARGET_ACTION_TYPE), is("submit"));
        assertThat(text(row, ClickstreamColumn.TARGET_SEMANTIC_ROLE), is("primary"));
        assertThat(text(row, ClickstreamColumn.TARGET_IDENTITY_SOURCE), is("dom-id"));
        assertThat(text(row, ClickstreamColumn.TARGET_FORM_ID), is("checkout"));
        assertThat(row.rqcid(), is("qvct4hxpuim9"));
        assertThat(row.eventType(), is("click"));
    }

    @Test
    public void legacyEventLeavesDomFieldsEmpty() {
        ClickstreamEventRecord row = map("""
                {"schemaVersion":1,"clientEventId":"abc123abc123abc123ab","type":"page_view","occurredAtMs":1710000000000,"script_id":"3jty6","payload":{"attribution":{"rqcid":"abc123xyz789"}}}
                """);

        assertDomEmpty(row);
        assertThat(row.eventType(), is("page_view"));
        assertThat(row.siteKey(), is("3jty6"));
        assertThat(row.rqcid(), is("abc123xyz789"));
        assertThat(row.occurredTs(), is(1710000000000L));
    }

    @Test
    public void partialDomFieldsLeaveTheRestEmpty() {
        ClickstreamEventRecord row = map(click("""
                "target_element_key": "hero",
                "target_element_type": "link"
                """));

        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_KEY), is("hero"));
        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_TYPE), is("link"));
        assertThat(text(row, ClickstreamColumn.TARGET_ACTION_TYPE), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_SEMANTIC_ROLE), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_IDENTITY_SOURCE), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_FORM_ID), is(""));
    }

    @Test
    public void nullAndEmptyDomFieldsStayEmpty() {
        ClickstreamEventRecord row = map(click("""
                "target_element_key": null,
                "target_element_type": "",
                "target_form_id": "   "
                """));

        assertDomEmpty(row);
    }

    @Test
    public void invalidDomTypesDoNotDropTheEvent() {
        ClickstreamEventRecord row = map(click("""
                "target_element_key": 12,
                "target_element_type": ["button"],
                "target_action_type": {"kind": "submit"},
                "target_semantic_role": "primary",
                "rqcid": "qvct4hxpuim9"
                """));

        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_KEY), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_ELEMENT_TYPE), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_ACTION_TYPE), is(""));
        assertThat(text(row, ClickstreamColumn.TARGET_SEMANTIC_ROLE), is("primary"));
        assertThat(row.rqcid(), is("qvct4hxpuim9"));
        assertThat(row.eventType(), is("click"));
    }

    @Test
    public void nonClickEventsKeepDomColumnsEmpty() {
        for (String type : List.of("page_view", "scroll", "form_submit", "page_engagement")) {
            ClickstreamEventRecord row = map("""
                    {"schemaVersion":1,"event_type":"%s","site_key":"fpxkp","occurred_ts":10}
                    """.formatted(type));
            assertThat(row.eventType(), is(type));
            assertDomEmpty(row);
        }
    }

    @Test
    public void newColumnsAreAppendedAndDoNotShiftExistingRowBinaryValues() {
        List<String> names = ClickstreamColumn.names();
        assertThat(names.get(names.size() - 6), is("target_element_key"));
        assertThat(names.get(names.size() - 1), is("target_form_id"));
        assertThat(names.get(names.indexOf("target_href_query_filtered") + 1), is("target_element_key"));

        ClickstreamEventRecord plain = map(click(""));
        ClickstreamEventRecord enriched = map(click("""
                "target_element_key": "cta-primary",
                "target_element_type": "button",
                "target_action_type": "submit",
                "target_semantic_role": "primary",
                "target_identity_source": "dom-id",
                "target_form_id": "checkout"
                """));
        plain.setEventId(7L);
        plain.setCapturedTs(8L);
        enriched.setEventId(7L);
        enriched.setCapturedTs(8L);

        byte[] plainBytes = ClickstreamRowBinary.encode(List.of(plain));
        byte[] enrichedBytes = ClickstreamRowBinary.encode(List.of(enriched));
        assertThat(tail(plainBytes, 6), is(new byte[] {0, 0, 0, 0, 0, 0}));
        int shared = plainBytes.length - 6;
        assertThat(prefix(enrichedBytes, shared), is(prefix(plainBytes, shared)));
        assertThat(readStrings(enrichedBytes, shared, 6), is(List.of(
                "cta-primary", "button", "submit", "primary", "dom-id", "checkout"
        )));
    }

    @Test
    public void batchKeepsLegacyAndEnrichedEventsTogether() {
        ClickstreamWriter writer = mock(ClickstreamWriter.class);
        strategy(writer).process(IngestBatch.of(List.of(
                TestIngest.json(1, """
                        {"schemaVersion":1,"type":"page_view","occurredAtMs":10,"script_id":"3jty6"}
                        """),
                TestIngest.json(2, click("""
                        "target_element_key": "cta-primary",
                        "target_element_type": "button"
                        """))
        ), FlushReason.SIZE));

        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(writer, times(1)).write(captor.capture());
        @SuppressWarnings("unchecked")
        List<ClickstreamEventRecord> rows = captor.getValue();
        assertThat(rows.size(), is(2));
        assertThat(rows.get(0).eventType(), is("page_view"));
        assertDomEmpty(rows.get(0));
        assertThat(rows.get(1).eventType(), is("click"));
        assertThat(text(rows.get(1), ClickstreamColumn.TARGET_ELEMENT_KEY), is("cta-primary"));
        assertThat(text(rows.get(1), ClickstreamColumn.TARGET_ELEMENT_TYPE), is("button"));
        assertThat(text(rows.get(1), ClickstreamColumn.TARGET_FORM_ID), is(""));
    }

    private ClickstreamEventRecord map(String json) {
        ClickstreamMapping mapping = new ClickstreamMapper(objectMapper, new ExactLongClientEventIdMapper())
                .map(objectMapper.readTree(json));
        assertThat(mapping.malformed(), is(false));
        assertThat(mapping.errors().isEmpty(), is(true));
        return mapping.events().getFirst();
    }

    private static String click(String extra) {
        String body = """
                {
                  "schemaVersion": 1,
                  "event_type": "click",
                  "site_key": "fpxkp",
                  "occurred_ts": 10
                """;
        if (extra.isBlank()) {
            return body + "}";
        }
        return body + "," + extra + "}";
    }

    private static void assertDomEmpty(ClickstreamEventRecord row) {
        for (ClickstreamColumn column : DOM) {
            assertThat(text(row, column), is(""));
        }
    }

    private static String text(ClickstreamEventRecord row, ClickstreamColumn column) {
        return (String) row.get(column);
    }

    private ClickstreamIngestStrategy strategy(ClickstreamWriter writer) {
        IngestMetrics metrics = TestIngest.metrics();
        return new ClickstreamIngestStrategy(
                new ClickstreamValidator(objectMapper),
                new ClickstreamMapper(objectMapper, new ExactLongClientEventIdMapper()),
                new ClickstreamEnrichmentPipeline(
                        Clock.fixed(Instant.parse("2026-10-22T00:00:00Z"), ZoneOffset.UTC),
                        () -> 42L,
                        new NoOpIpAddressResolver(metrics),
                        new NoOpGeoEnrichmentService(metrics),
                        new NoOpNetworkEnrichmentService(metrics),
                        new NoOpClickstreamAttributionResolver(metrics)
                ),
                writer,
                mock(DeadLetterPublisher.class),
                metrics
        );
    }

    private static byte[] prefix(byte[] bytes, int length) {
        byte[] prefix = new byte[length];
        System.arraycopy(bytes, 0, prefix, 0, length);
        return prefix;
    }

    private static byte[] tail(byte[] bytes, int length) {
        byte[] tail = new byte[length];
        System.arraycopy(bytes, bytes.length - length, tail, 0, length);
        return tail;
    }

    private static List<String> readStrings(byte[] bytes, int offset, int count) {
        List<String> values = new ArrayList<>();
        int cursor = offset;
        for (int i = 0; i < count; i++) {
            int length = 0;
            int shift = 0;
            while (true) {
                int piece = bytes[cursor++] & 0xff;
                length |= (piece & 0x7f) << shift;
                if ((piece & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            values.add(new String(bytes, cursor, length, StandardCharsets.UTF_8));
            cursor += length;
        }
        assertThat(cursor, is(bytes.length));
        return values;
    }
}
