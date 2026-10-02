package ru.refiq.strategy.clickstream.enrichment;

public record NetworkEnrichment(long asn, byte datacenter, String proxyType) {

    public static NetworkEnrichment empty() {
        return new NetworkEnrichment(0L, (byte) -1, "");
    }
}
