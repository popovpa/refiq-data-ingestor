package ru.refiq.strategy.clickstream.enrichment;

public record GeoEnrichment(String countryCode, String region, String city) {

    public static GeoEnrichment empty() {
        return new GeoEnrichment("", "", "");
    }
}
