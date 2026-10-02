package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.error.InvalidRecordException;
import ru.refiq.strategy.clickstream.ClickstreamEventRecord;

import java.time.Clock;

@Component
@ConditionalOnClickstreamPipeline
public class ClickstreamEnrichmentPipeline {

    private final Clock clock;
    private final EventIdGenerator eventIdGenerator;
    private final IpAddressResolver ipAddressResolver;
    private final GeoEnrichmentService geoEnrichmentService;
    private final NetworkEnrichmentService networkEnrichmentService;
    private final ClickstreamAttributionResolver attributionResolver;

    public ClickstreamEnrichmentPipeline(
            Clock clock,
            EventIdGenerator eventIdGenerator,
            IpAddressResolver ipAddressResolver,
            GeoEnrichmentService geoEnrichmentService,
            NetworkEnrichmentService networkEnrichmentService,
            ClickstreamAttributionResolver attributionResolver
    ) {
        this.clock = clock;
        this.eventIdGenerator = eventIdGenerator;
        this.ipAddressResolver = ipAddressResolver;
        this.geoEnrichmentService = geoEnrichmentService;
        this.networkEnrichmentService = networkEnrichmentService;
        this.attributionResolver = attributionResolver;
    }

    public void enrich(ClickstreamEventRecord record) {
        enrich(TrustedClickstreamContext.none(), record);
    }

    public void enrich(TrustedClickstreamContext context, ClickstreamEventRecord record) {
        long capturedTs = context.capturedTs() != null && context.capturedTs() > 0L
                ? context.capturedTs()
                : clock.millis();
        if (capturedTs <= 0L) {
            throw new InvalidRecordException("captured_ts is invalid");
        }
        record.setCapturedTs(capturedTs);
        long eventId = context.eventId() != null && context.eventId() != 0L
                ? context.eventId()
                : eventIdGenerator.nextId();
        record.setEventId(eventId);
        record.setIpAddress(ipAddressResolver.resolve(context));
        GeoEnrichment geo = geoEnrichmentService.lookup(context);
        record.setGeoCountryCode(geo.countryCode());
        record.setGeoRegion(geo.region());
        record.setGeoCity(geo.city());
        NetworkEnrichment network = networkEnrichmentService.lookup(context);
        record.setNetworkAsn(network.asn());
        record.setNetworkIsDatacenter(network.datacenter());
        record.setNetworkProxyType(network.proxyType());
        ClickstreamAttribution attribution = attributionResolver.resolve(context);
        record.setSiteId(attribution.siteId());
        record.setBusinessId(attribution.businessId());
        record.setPartnerId(attribution.partnerId());
        record.setOfferId(attribution.offerId());
        record.setCampaignId(attribution.campaignId());
        record.setTrackingLinkId(attribution.trackingLinkId());
        record.setTrafficOwnership(attribution.trafficOwnership());
    }
}
