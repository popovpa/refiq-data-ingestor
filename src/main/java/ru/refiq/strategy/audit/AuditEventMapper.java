package ru.refiq.strategy.audit;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnAuditPipeline;
import ru.refiq.error.InvalidRecordException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

@Component
@ConditionalOnAuditPipeline
public class AuditEventMapper {

    private static final Set<String> ACTOR_TYPES = Set.of("USER", "ADMIN", "SYSTEM", "SERVICE", "API_CLIENT");
    private static final Set<String> ACTIONS = Set.of(
            "CREATE", "UPDATE", "DELETE", "STATUS_CHANGE", "APPROVE", "REJECT",
            "ENABLE", "DISABLE", "PERMISSION_CHANGE", "PAYMENT_ACTION", "SYSTEM_ACTION"
    );
    private static final int SHORT_TEXT = 128;
    private static final int ACTION_TEXT = 64;
    private static final int USER_AGENT = 4096;
    private static final int JSON_TEXT = 65536;

    private final ObjectMapper objectMapper;

    public AuditEventMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public AuditEvent map(JsonNode node) {
        int schemaVersion = requiredInt(node, "schemaVersion");
        if (schemaVersion != AuditEventValidator.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidRecordException("unsupported schema version");
        }
        String eventId = requiredText(node, "eventId", SHORT_TEXT);
        String eventType = requiredText(node, "eventType", SHORT_TEXT);
        String action = requiredText(node, "action", ACTION_TEXT);
        if (!ACTIONS.contains(action)) {
            throw new InvalidRecordException("action is invalid");
        }
        Instant occurredAt = occurredAt(node);
        JsonNode actor = requiredObject(node, "actor");
        String actorType = requiredText(actor, "type", ACTION_TEXT);
        if (!ACTOR_TYPES.contains(actorType)) {
            throw new InvalidRecordException("actor.type is invalid");
        }
        JsonNode entity = requiredObject(node, "entity");
        return new AuditEvent(
                eventId,
                schemaVersion,
                eventType,
                action,
                occurredAt,
                optionalText(node, "accountId", SHORT_TEXT),
                actorType,
                optionalText(actor, "id", SHORT_TEXT),
                requiredText(entity, "type", SHORT_TEXT),
                requiredText(entity, "id", SHORT_TEXT),
                optionalText(node, "requestId", SHORT_TEXT),
                optionalText(node, "correlationId", SHORT_TEXT),
                optionalText(node, "ipAddress", SHORT_TEXT),
                optionalText(node, "userAgent", USER_AGENT),
                json(node.get("changes")),
                json(node.get("before")),
                json(node.get("after")),
                json(node.get("metadata"))
        );
    }

    private String json(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isObject()) {
            throw new InvalidRecordException("json block must be an object");
        }
        try {
            String encoded = objectMapper.writeValueAsString(node);
            if (encoded.length() > JSON_TEXT) {
                throw new InvalidRecordException("json block is too long");
            }
            return encoded;
        } catch (JacksonException e) {
            throw new InvalidRecordException("json block cannot be encoded");
        }
    }

    private static Instant occurredAt(JsonNode node) {
        JsonNode value = node.get("occurredAt");
        if (value == null || value.isNull() || !value.isTextual()) {
            throw new InvalidRecordException("occurredAt is missing");
        }
        try {
            return Instant.parse(value.asText("").trim());
        } catch (DateTimeParseException e) {
            throw new InvalidRecordException("occurredAt is invalid");
        }
    }

    private static JsonNode requiredObject(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isObject()) {
            throw new InvalidRecordException(field + " is missing");
        }
        return value;
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() <= 0) {
            throw new InvalidRecordException(field + " is missing or invalid");
        }
        return value.intValue();
    }

    private static String requiredText(JsonNode node, String field, int max) {
        String text = optionalText(node, field, max);
        if (text == null) {
            throw new InvalidRecordException(field + " is missing");
        }
        return text;
    }

    private static String optionalText(JsonNode node, String field, int max) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (!value.isTextual()) {
            throw new InvalidRecordException(field + " is invalid");
        }
        String text = value.asText("").trim();
        if (text.isEmpty()) {
            return null;
        }
        if (text.length() > max) {
            throw new InvalidRecordException(field + " is too long");
        }
        return text;
    }
}
