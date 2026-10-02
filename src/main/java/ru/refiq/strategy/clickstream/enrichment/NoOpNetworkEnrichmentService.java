package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.metrics.IngestMetrics;

@Component
public class NoOpNetworkEnrichmentService implements NetworkEnrichmentService {

    private final IngestMetrics metrics;

    public NoOpNetworkEnrichmentService(IngestMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public NetworkEnrichment lookup(TrustedClickstreamContext context) {
        metrics.enrichmentStub("network");
        if (context.asn() == null && context.datacenter() == null && context.proxyType() == null) {
            return NetworkEnrichment.empty();
        }
        return new NetworkEnrichment(
                context.asn() == null ? 0L : context.asn(),
                context.datacenter() == null ? (byte) -1 : context.datacenter(),
                context.proxyType() == null ? "" : context.proxyType()
        );
    }
}
