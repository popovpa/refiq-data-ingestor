package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.clickstream.ClickstreamAddresses;

@Component
@ConditionalOnClickstreamPipeline
public class NoOpIpAddressResolver implements IpAddressResolver {

    private final IngestMetrics metrics;

    public NoOpIpAddressResolver(IngestMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public String resolve(TrustedClickstreamContext context) {
        metrics.enrichmentStub("ip");
        String trusted = context.ipAddress();
        if (trusted == null || trusted.isBlank()) {
            return "::";
        }
        return ClickstreamAddresses.canonicalOrUnspecified(trusted);
    }
}
