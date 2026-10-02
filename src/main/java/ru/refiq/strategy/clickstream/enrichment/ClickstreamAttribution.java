package ru.refiq.strategy.clickstream.enrichment;

public record ClickstreamAttribution(
        long siteId,
        long businessId,
        long partnerId,
        long offerId,
        long campaignId,
        long trackingLinkId,
        String trafficOwnership
) {

    public static ClickstreamAttribution empty() {
        return new ClickstreamAttribution(0L, 0L, 0L, 0L, 0L, 0L, "");
    }
}
