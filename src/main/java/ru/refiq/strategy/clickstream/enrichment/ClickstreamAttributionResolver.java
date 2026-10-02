package ru.refiq.strategy.clickstream.enrichment;

public interface ClickstreamAttributionResolver {

    /**
     * Future lookup may use site_key and rqcid against trusted server-side data.
     * Browser payload is not a source for these ids.
     */
    ClickstreamAttribution resolve(TrustedClickstreamContext context);
}
