package ru.refiq.strategy.clickstream.enrichment;

public interface IpAddressResolver {

    /**
     * Client IP already taken by the tracker from the ingress {@code X-Real-IP} header
     * and written onto the Kafka event. This consumer has no HTTP request.
     */
    String resolve(TrustedClickstreamContext context);
}
