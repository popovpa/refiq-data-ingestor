package ru.refiq.strategy.clickstream;

import org.testng.annotations.Test;
import ru.refiq.error.InvalidRecordException;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamAttribution;
import ru.refiq.strategy.clickstream.enrichment.ClickstreamEnrichmentPipeline;
import ru.refiq.strategy.clickstream.enrichment.GeoEnrichment;
import ru.refiq.strategy.clickstream.enrichment.NetworkEnrichment;
import ru.refiq.strategy.clickstream.enrichment.NoOpClickstreamAttributionResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpGeoEnrichmentService;
import ru.refiq.strategy.clickstream.enrichment.NoOpIpAddressResolver;
import ru.refiq.strategy.clickstream.enrichment.NoOpNetworkEnrichmentService;
import ru.refiq.strategy.clickstream.enrichment.TrustedClickstreamContext;
import ru.refiq.support.TestIngest;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.testng.Assert.expectThrows;

public class ClickstreamFlatMappingTest {

    private static final String FULL = """
            {
              "schemaVersion": 1,
              "clientEventId": 9007199254740993,
              "batchId": 5,
              "type": "custom",
              "customEventName": "signup",
              "sequence": 7,
              "occurredAtMs": 1710000000000,
              "sentTs": 1710000001000,
              "capturedTs": 1,
              "visitorId": 11,
              "sessionId": 12,
              "pageViewId": 13,
              "previousPageViewId": 14,
              "script_id": "3jty6",
              "siteId": 99,
              "businessId": 98,
              "rqcid": "abc123xyz789",
              "rqcidCapturedTs": 20,
              "utmSource": "newsletter",
              "utmMedium": "email",
              "utmCampaign": "spring",
              "yclid": "y",
              "landing": {"origin": "https://landing.example", "hostname": "landing.example", "pathname": "/start"},
              "page": {
                "protocol": "https",
                "origin": "https://example.com",
                "hostname": "example.com",
                "port": 443,
                "pathname": "/pricing",
                "hashRoute": "/plans",
                "title": "Pricing",
                "contentType": "text/html",
                "charset": "utf-8",
                "navigationType": "navigate",
                "historyLength": 3,
                "isTopFrame": true,
                "readyState": "complete",
                "visibilityState": "visible",
                "documentWidth": 1200,
                "documentHeight": 4000,
                "referrerOrigin": "https://ref.example",
                "referrerHostname": "ref.example",
                "referrerPathname": "/from"
              },
              "navigator": {
                "userAgent": "Agent",
                "language": "ru",
                "languages": ["ru", "en"],
                "platform": "MacIntel",
                "vendor": "Apple",
                "cookieEnabled": false,
                "webdriver": false,
                "pdfViewerEnabled": true,
                "hardwareConcurrency": 8,
                "deviceMemoryGb": 8,
                "maxTouchPoints": 0,
                "uaMobile": false,
                "uaPlatform": "macOS",
                "uaBrands": "Chromium"
              },
              "screen": {"width": 1920, "height": 1080, "availWidth": 1920, "availHeight": 1000, "colorDepth": 24, "pixelDepth": 24, "orientationType": "landscape", "orientationAngle": 0, "devicePixelRatio": 2},
              "viewport": {"width": 1280, "height": 720},
              "window": {"outerWidth": 1300, "outerHeight": 800},
              "visualViewport": {"width": 1280, "height": 700, "scale": 1, "offsetLeft": 0, "offsetTop": 10},
              "mediaFeatures": {"colorScheme": "dark", "reducedMotion": true, "pointer": "fine", "hover": true, "displayMode": "browser"},
              "locale": "ru-RU",
              "timezone": "Europe/Moscow",
              "timezoneOffsetMinutes": -180,
              "network": {"online": true, "type": "wifi", "effectiveType": "4g", "downlinkMbps": 10.5, "rtt": 50, "saveData": false},
              "performance": {"timeOriginTs": 100, "redirectCount": 1, "ttfb": 30, "domContentLoaded": 200, "load": 400, "nextHopProtocol": "h2", "transferSize": 5000},
              "webVitals": {"fp": 10, "fcp": 20, "lcp": 2500, "cls": 0.1, "inp": 40},
              "resources": {"count": 4, "scriptCount": 2, "imageCount": 1, "failedCount": 0, "transferBytes": 1000, "slowest": 80},
              "click": {"clientX": 3, "clientY": 4, "pageX": 5, "pageY": 6, "relativeX": 0.2, "relativeY": 0.3, "button": 0, "pointerType": "mouse", "ctrlKey": true, "shiftKey": false, "altKey": false, "metaKey": false},
              "pointer": {"pressure": 0.4, "width": 1, "height": 1, "isPrimary": true},
              "target": {"trackingId": "cta", "elementId": "buy", "tag": "a", "role": "link", "hrefOrigin": "https://shop.example", "hrefHostname": "shop.example", "hrefPathname": "/buy", "isExternal": false, "width": 40, "height": 20},
              "scroll": {"x": 0, "y": 100, "documentHeight": 4000, "viewportHeight": 700, "depth": 0.5, "maxDepth": 0.75, "direction": "down", "milestone": 50, "timeTo25": 5, "timeTo50": 10, "timeTo75": -1, "timeTo90": -1, "timeTo100": -1},
              "engagement": {"totalDuration": 2000, "visibleDuration": 1500, "activeDuration": 800, "timeToFirstInteraction": 100, "clickCount": 2, "scrollCount": 3, "maxScrollDepth": 0.75, "exitReason": "hidden"},
              "form": {"trackingId": "lead", "elementId": "form", "fieldCount": 3, "valid": true, "method": "post"},
              "media": {"trackingId": "vid", "type": "video", "action": "play", "position": 15, "duration": 90, "percent": 0.2},
              "error": {"type": "TypeError", "fingerprint": 99, "scriptPath": "/app.js", "line": 12, "column": 4},
              "privacy": {"globalPrivacyControl": true, "doNotTrack": false},
              "consent": {"analytics": "granted", "updatedTs": 30, "policyVersion": "v1"},
              "sdk": {"version": "2.0.0", "integrationMode": "script"},
              "batchEventCount": 2,
              "batchBytes": 400,
              "delivery": {"transport": "beacon", "attempt": 2, "fromIndexeddb": true, "finalFlush": false},
              "requestId": 77,
              "http": {"userAgent": "ServerUA", "origin": "https://example.com", "referer": "https://ref.example/", "acceptLanguage": "ru", "version": "2", "contentType": "application/json", "requestBytes": 120},
              "ipAddress": "203.0.113.8",
              "geoCountryCode": "US",
              "networkAsn": 15169,
              "customPayloadRaw": "{\\"plan\\":\\"pro\\"}"
            }
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ClickstreamMapper mapper = new ClickstreamMapper(objectMapper, new ExactLongClientEventIdMapper());

    @Test
    public void mapsBaseFieldsTimestampsAndLongIds() {
        ClickstreamEventRecord record = full();

        assertThat(record.schemaVersion(), is(1L));
        assertThat(record.eventType(), is("custom"));
        assertThat(record.customEventName(), is("signup"));
        assertThat(record.sequence(), is(7L));
        assertThat(record.occurredTs(), is(1_710_000_000_000L));
        assertThat(record.sentTs(), is(1_710_000_001_000L));
        assertThat(record.clientEventId(), is(9_007_199_254_740_993L));
        assertThat(record.batchId(), is(5L));
        assertThat(record.visitorId(), is(11L));
        assertThat(record.sessionId(), is(12L));
        assertThat(record.pageViewId(), is(13L));
        assertThat(record.previousPageViewId(), is(14L));
        assertThat(record.siteKey(), is("3jty6"));
        assertThat(record.rqcid(), is("abc123xyz789"));
        assertThat(record.rqcidCapturedTs(), is(20L));
        assertThat(record.utmSource(), is("newsletter"));
        assertThat(record.requestId(), is(77L));
    }

    @Test
    public void keepsClickHouseDefaultsForMissingIdsBooleansAndMetrics() {
        ClickstreamEventRecord record = map("""
                {"schemaVersion":1,"type":"page_view","occurredAtMs":0,"script_id":"3jty6"}
                """);

        assertThat(record.clientEventId(), is(0L));
        assertThat(record.visitorId(), is(0L));
        assertThat(record.sessionId(), is(0L));
        assertThat(record.pageViewId(), is(0L));
        assertThat(record.siteId(), is(0L));
        assertThat(record.businessId(), is(0L));
        assertThat(record.pageIsTopFrame(), is(-1L));
        assertThat(record.navigatorCookieEnabled(), is(-1L));
        assertThat(record.networkOnline(), is(-1L));
        assertThat(record.webVitalLcp(), is(-1L));
        assertThat(record.webVitalCls(), is(-1.0f));
        assertThat(record.clickClientX(), is(-1L));
        assertThat(record.scrollDepth(), is(-1.0f));
        assertThat(record.formValid(), is(-1L));
        assertThat(record.mediaPercent(), is(-1.0f));
        assertThat(record.performanceTtfb(), is(-1L));
        assertThat(record.deliveryFromIndexeddb(), is(0L));
        assertThat(record.ipAddress(), is("::"));
        assertThat(record.geoCountryCode(), is(""));
        assertThat(record.networkAsn(), is(0L));
        assertThat(record.networkIsDatacenter(), is(-1L));
        assertThat(record.customPayloadRaw(), is(""));
        assertThat(record.customPayloadBytes(), is(0L));
        assertThat(record.capturedTs(), is(0L));
        assertThat(record.eventId(), is(0L));
    }

    @Test
    public void legacyStringClientEventIdIsNotReplaced() {
        ClickstreamEventRecord record = map("""
                {"schemaVersion":1,"clientEventId":"abc123abc123abc123ab","type":"page_view","occurredAtMs":10,"script_id":"3jty6"}
                """);

        assertThat(record.clientEventId(), is(0L));
    }

    @Test
    public void opaqueVisitorIdStaysZero() {
        ClickstreamEventRecord record = map("""
                {"schemaVersion":1,"type":"page_view","occurredAtMs":10,"script_id":"3jty6","visitorId":"not-a-long"}
                """);

        assertThat(record.visitorId(), is(0L));
    }

    @Test
    public void mapsPageNavigatorScreenAndNetwork() {
        ClickstreamEventRecord record = full();

        assertThat(record.pageHostname(), is("example.com"));
        assertThat(record.pagePathname(), is("/pricing"));
        assertThat(record.pageTitle(), is("Pricing"));
        assertThat(record.pagePort(), is(443L));
        assertThat(record.pageIsTopFrame(), is(1L));
        assertThat(record.landingHostname(), is("landing.example"));
        assertThat(record.navigatorUserAgent(), is("Agent"));
        assertThat(record.navigatorLanguage(), is("ru"));
        assertThat(record.navigatorLanguages(), is("ru,en"));
        assertThat(record.navigatorCookieEnabled(), is(0L));
        assertThat(record.navigatorDeviceMemoryGb(), is(8.0f));
        assertThat(record.screenWidth(), is(1920L));
        assertThat(record.screenDevicePixelRatio(), is(2.0f));
        assertThat(record.viewportWidth(), is(1280L));
        assertThat(record.windowOuterWidth(), is(1300L));
        assertThat(record.visualViewportOffsetTop(), is(10.0f));
        assertThat(record.networkOnline(), is(1L));
        assertThat(record.networkEffectiveType(), is("4g"));
        assertThat(record.networkRtt(), is(50L));
        assertThat(record.networkDownlinkMbps(), is(10.5f));
        assertThat(record.locale(), is("ru-RU"));
        assertThat(record.timezone(), is("Europe/Moscow"));
    }

    @Test
    public void mapsPerformanceWebVitalsAndResources() {
        ClickstreamEventRecord record = full();

        assertThat(record.performanceTtfb(), is(30L));
        assertThat(record.performanceLoad(), is(400L));
        assertThat(record.performanceTransferSize(), is(5000L));
        assertThat(record.webVitalFp(), is(10L));
        assertThat(record.webVitalFcp(), is(20L));
        assertThat(record.webVitalLcp(), is(2500L));
        assertThat(record.webVitalCls(), is(0.1f));
        assertThat(record.webVitalInp(), is(40L));
        assertThat(record.resourceCount(), is(4L));
        assertThat(record.resourceTransferBytes(), is(1000L));
        assertThat(record.resourceSlowest(), is(80L));
    }

    @Test
    public void mapsInteractionFormMediaErrorPrivacyAndDelivery() {
        ClickstreamEventRecord record = full();

        assertThat(record.clickClientX(), is(3L));
        assertThat(record.clickCtrlKey(), is(1L));
        assertThat(record.clickShiftKey(), is(0L));
        assertThat(record.pointerIsPrimary(), is(1L));
        assertThat(record.targetTag(), is("a"));
        assertThat(record.targetTrackingId(), is("cta"));
        assertThat(record.targetIsExternal(), is(0L));
        assertThat(record.scrollDepth(), is(0.5f));
        assertThat(record.scrollMilestone(), is(50L));
        assertThat(record.scrollTimeTo50(), is(10L));
        assertThat(record.engagementVisibleDuration(), is(1500L));
        assertThat(record.engagementClickCount(), is(2L));
        assertThat(record.engagementExitReason(), is("hidden"));
        assertThat(record.formTrackingId(), is("lead"));
        assertThat(record.formFieldCount(), is(3L));
        assertThat(record.formValid(), is(1L));
        assertThat(record.mediaTrackingId(), is("vid"));
        assertThat(record.mediaType(), is("video"));
        assertThat(record.mediaAction(), is("play"));
        assertThat(record.mediaPosition(), is(15L));
        assertThat(record.mediaPercent(), is(0.2f));
        assertThat(record.errorType(), is("TypeError"));
        assertThat(record.errorFingerprint(), is(99L));
        assertThat(record.errorLine(), is(12L));
        assertThat(record.errorColumn(), is(4L));
        assertThat(record.privacyGlobalPrivacyControl(), is(1L));
        assertThat(record.privacyDoNotTrack(), is(0L));
        assertThat(record.consentAnalytics(), is("granted"));
        assertThat(record.consentUpdatedTs(), is(30L));
        assertThat(record.sdkVersion(), is("2.0.0"));
        assertThat(record.sdkIntegrationMode(), is("script"));
        assertThat(record.deliveryTransport(), is("beacon"));
        assertThat(record.deliveryAttempt(), is(2L));
        assertThat(record.deliveryFromIndexeddb(), is(1L));
        assertThat(record.deliveryFinalFlush(), is(0L));
        assertThat(record.httpUserAgent(), is("ServerUA"));
        assertThat(record.httpOrigin(), is("https://example.com"));
    }

    @Test
    public void customPayloadBytesAreUtf8Length() {
        ClickstreamEventRecord record = full();
        String raw = "{\"plan\":\"pro\"}";

        assertThat(record.customPayloadRaw(), is(raw));
        assertThat(record.customPayloadBytes(), is((long) raw.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    public void doesNotTrustBrowserServerFields() {
        ClickstreamEventRecord record = full();

        assertThat(record.capturedTs(), is(0L));
        assertThat(record.eventId(), is(0L));
        assertThat(record.siteId(), is(0L));
        assertThat(record.businessId(), is(0L));
        assertThat(record.ipAddress(), is("::"));
        assertThat(record.geoCountryCode(), is(""));
        assertThat(record.networkAsn(), is(0L));
    }

    @Test
    public void rejectsOversizedStrings() {
        String title = "t".repeat(2049);
        ClickstreamMapping mapping = mapper.map(objectMapper.readTree("""
                {"schemaVersion":1,"type":"page_view","occurredAtMs":1,"script_id":"3jty6","pageTitle":"%s"}
                """.formatted(title)));

        assertThat(mapping.malformed(), is(true));
        assertThat(mapping.errors().getFirst(), containsString("too long"));
    }

    @Test
    public void columnOrderMatchesDdl() throws IOException {
        String sql = new String(getClass().getResourceAsStream("/clickhouse/clickstream_events.sql").readAllBytes(), StandardCharsets.UTF_8);
        List<String> fromSql = new ArrayList<>();
        for (String line : sql.split("\n")) {
            if (line.startsWith("    ") && line.trim().matches("[a-z_][a-z0-9_]* .*")) {
                fromSql.add(line.trim().split(" ")[0]);
            }
        }

        assertThat(ClickstreamColumn.names(), is(fromSql));
    }

    @Test
    public void capturedTsAndEventIdComeFromServer() {
        Clock clock = Clock.fixed(Instant.parse("2024-06-01T00:00:00Z"), ZoneOffset.UTC);
        ClickstreamEnrichmentPipeline pipeline = pipeline(clock, () -> 42L);
        ClickstreamEventRecord record = full();

        pipeline.enrich(record);

        assertThat(record.capturedTs(), is(clock.millis()));
        assertThat(record.eventId(), is(42L));
        assertThat(record.ipAddress(), is("::"));
        assertThat(record.geoCountryCode(), is(""));
        assertThat(record.geoRegion(), is(""));
        assertThat(record.geoCity(), is(""));
        assertThat(record.networkAsn(), is(0L));
        assertThat(record.networkIsDatacenter(), is(-1L));
        assertThat(record.networkProxyType(), is(""));
        assertThat(record.siteId(), is(0L));
        assertThat(record.trafficOwnership(), is(""));
    }

    @Test
    public void stubsUseTrustedContextWhenPresent() {
        IngestMetrics metrics = TestIngest.metrics();
        TrustedClickstreamContext trusted = new TrustedClickstreamContext(
                50L, 60L, "203.0.113.9", "DE", "BE", "Berlin", 64500L, (byte) 0, "none",
                3L, 4L, 5L, 6L, 7L, 8L, "partner"
        );

        assertThat(new NoOpIpAddressResolver(metrics).resolve(TrustedClickstreamContext.none()), is("::"));
        assertThat(new NoOpIpAddressResolver(metrics).resolve(trusted), is("203.0.113.9"));
        assertThat(new NoOpGeoEnrichmentService(metrics).lookup(TrustedClickstreamContext.none()), is(GeoEnrichment.empty()));
        assertThat(new NoOpGeoEnrichmentService(metrics).lookup(trusted).countryCode(), is("DE"));
        assertThat(new NoOpNetworkEnrichmentService(metrics).lookup(TrustedClickstreamContext.none()), is(NetworkEnrichment.empty()));
        assertThat(new NoOpNetworkEnrichmentService(metrics).lookup(trusted).asn(), is(64500L));
        assertThat(new NoOpClickstreamAttributionResolver(metrics).resolve(TrustedClickstreamContext.none()), is(ClickstreamAttribution.empty()));
        assertThat(new NoOpClickstreamAttributionResolver(metrics).resolve(trusted).siteId(), is(3L));
        assertThat(new NoOpClickstreamAttributionResolver(metrics).resolve(trusted).trafficOwnership(), is("partner"));
    }

    @Test
    public void negativeOccurredTsIsInvalid() {
        ClickstreamMapping mapping = mapper.map(objectMapper.readTree("""
                {"schemaVersion":1,"type":"page_view","occurredAtMs":-1,"script_id":"3jty6"}
                """));

        assertThat(mapping.malformed(), is(true));
    }

    @Test
    public void exactLongRejectsOverflowingClientEventId() {
        expectThrows(InvalidRecordException.class, () -> new ExactLongClientEventIdMapper().map(
                objectMapper.readTree("\"9223372036854775808\"")
        ));
    }

    private ClickstreamEventRecord full() {
        return map(FULL);
    }

    private ClickstreamEventRecord map(String json) {
        ClickstreamMapping mapping = mapper.map(objectMapper.readTree(json));
        assertThat(mapping.malformed(), is(false));
        assertThat(mapping.errors().isEmpty(), is(true));
        return mapping.events().getFirst();
    }

    private static ClickstreamEnrichmentPipeline pipeline(Clock clock, ru.refiq.strategy.clickstream.enrichment.EventIdGenerator ids) {
        IngestMetrics metrics = TestIngest.metrics();
        return new ClickstreamEnrichmentPipeline(
                clock,
                ids,
                new NoOpIpAddressResolver(metrics),
                new NoOpGeoEnrichmentService(metrics),
                new NoOpNetworkEnrichmentService(metrics),
                new NoOpClickstreamAttributionResolver(metrics)
        );
    }
}
