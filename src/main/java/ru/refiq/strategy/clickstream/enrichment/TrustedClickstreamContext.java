package ru.refiq.strategy.clickstream.enrichment;

/**
 * Server-side fields already trusted by an internal producer.
 * The Kafka payload of the current SDK is not this context.
 */
public record TrustedClickstreamContext(
        Long capturedTs,
        Long eventId,
        String ipAddress,
        String countryCode,
        String region,
        String city,
        Long asn,
        Byte datacenter,
        String proxyType,
        Long siteId,
        Long businessId,
        Long partnerId,
        Long offerId,
        Long campaignId,
        Long trackingLinkId,
        String trafficOwnership
) {

    public static TrustedClickstreamContext none() {
        return new TrustedClickstreamContext(
                null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null
        );
    }
}
