package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.metrics.IngestMetrics;

@Component
public class NoOpClickstreamAttributionResolver implements ClickstreamAttributionResolver {

    private final IngestMetrics metrics;

    public NoOpClickstreamAttributionResolver(IngestMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public ClickstreamAttribution resolve(TrustedClickstreamContext context) {
        metrics.enrichmentStub("attribution");
        if (context.siteId() == null
                && context.businessId() == null
                && context.partnerId() == null
                && context.offerId() == null
                && context.campaignId() == null
                && context.trackingLinkId() == null
                && context.trafficOwnership() == null) {
            return ClickstreamAttribution.empty();
        }
        return new ClickstreamAttribution(
                zero(context.siteId()),
                zero(context.businessId()),
                zero(context.partnerId()),
                zero(context.offerId()),
                zero(context.campaignId()),
                zero(context.trackingLinkId()),
                context.trafficOwnership() == null ? "" : context.trafficOwnership()
        );
    }

    private static long zero(Long value) {
        return value == null ? 0L : value;
    }
}
