package ru.refiq.strategy.clickstream.enrichment;

public interface NetworkEnrichmentService {

    /**
     * TODO: future ASN, datacenter and proxy classification from a trusted client IP.
     * Do not add VPN, Tor or reputation lookups here yet.
     */
    NetworkEnrichment lookup(TrustedClickstreamContext context);
}
