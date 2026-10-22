package ru.refiq.strategy.clickstream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.refiq.error.InvalidRecordException;
import ru.refiq.strategy.clickstream.enrichment.TrustedClickstreamContext;
import tools.jackson.databind.JsonNode;

import java.util.Set;

/**
 * Reads server-owned entity fields from the canonical Kafka document.
 * Nested browser groups are ignored. {@code skipBrowserBind} still blocks the generic binder.
 */
public final class CanonicalKafkaEntityContext {

    private static final Logger log = LoggerFactory.getLogger(CanonicalKafkaEntityContext.class);
    private static final Set<String> OWNERSHIP = Set.of("", "partner", "business");

    private CanonicalKafkaEntityContext() {
    }

    public static TrustedClickstreamContext from(JsonNode event, JsonNode envelope) {
        return new TrustedClickstreamContext(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                id(event, envelope, "site_id", "siteId"),
                id(event, envelope, "business_id", "businessId"),
                id(event, envelope, "partner_id", "partnerId"),
                id(event, envelope, "offer_id", "offerId"),
                id(event, envelope, "campaign_id", "campaignId"),
                id(event, envelope, "tracking_link_id", "trackingLinkId"),
                ownership(event, envelope)
        );
    }

    private static long id(JsonNode event, JsonNode envelope, String snake, String camel) {
        JsonNode value = topLevel(event, snake, camel);
        if (value == null && envelope != event) {
            value = topLevel(envelope, snake, camel);
        }
        if (value == null) {
            return 0L;
        }
        Long number = ClickstreamNumbers.readLong(value, snake);
        if (number == null) {
            return 0L;
        }
        if (number < 0L) {
            log.warn("invalid canonical entity field field={} reason=negative", snake);
            throw new InvalidRecordException(snake + " is invalid");
        }
        return number;
    }

    private static String ownership(JsonNode event, JsonNode envelope) {
        JsonNode value = topLevel(event, "traffic_ownership", "trafficOwnership");
        if (value == null && envelope != event) {
            value = topLevel(envelope, "traffic_ownership", "trafficOwnership");
        }
        if (value == null) {
            return "";
        }
        if (!value.isTextual()) {
            log.warn("invalid canonical entity field field=traffic_ownership reason=type");
            throw new InvalidRecordException("traffic_ownership is invalid");
        }
        String text = value.asText("").trim();
        if (!OWNERSHIP.contains(text)) {
            log.warn("invalid canonical entity field field=traffic_ownership reason=value");
            throw new InvalidRecordException("traffic_ownership is invalid");
        }
        return text;
    }

    private static JsonNode topLevel(JsonNode node, String snake, String camel) {
        if (node == null || !node.isObject()) {
            return null;
        }
        JsonNode value = node.get(snake);
        if (value == null || value.isNull()) {
            value = node.get(camel);
        }
        if (value == null || value.isNull() || value.isContainer()) {
            return null;
        }
        return value;
    }
}
