package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.metrics.IngestMetrics;

@Component
@ConditionalOnClickstreamPipeline
public class NoOpGeoEnrichmentService implements GeoEnrichmentService {

    private final IngestMetrics metrics;

    public NoOpGeoEnrichmentService(IngestMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public GeoEnrichment lookup(TrustedClickstreamContext context) {
        metrics.enrichmentStub("geo");
        if (context.countryCode() == null && context.region() == null && context.city() == null) {
            return GeoEnrichment.empty();
        }
        return new GeoEnrichment(blank(context.countryCode()), blank(context.region()), blank(context.city()));
    }

    private static String blank(String value) {
        return value == null ? "" : value;
    }
}
