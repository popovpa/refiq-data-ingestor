package ru.refiq.strategy.clickstream.enrichment;

public interface GeoEnrichmentService {

    /**
     * TODO: future GeoIP enrichment based on trusted client IP.
     */
    GeoEnrichment lookup(TrustedClickstreamContext context);
}
