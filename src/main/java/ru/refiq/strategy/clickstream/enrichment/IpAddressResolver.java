package ru.refiq.strategy.clickstream.enrichment;

public interface IpAddressResolver {

    /**
     * TODO: resolve trusted client IP using configured trusted proxy chain.
     * Do not parse X-Forwarded-For, Forwarded or X-Real-IP here until that chain exists.
     */
    String resolve(TrustedClickstreamContext context);
}
