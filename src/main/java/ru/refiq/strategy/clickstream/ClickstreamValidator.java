package ru.refiq.strategy.clickstream;

import org.springframework.stereotype.Component;
import ru.refiq.error.InvalidRecordException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
public class ClickstreamValidator {

    static final int SUPPORTED_SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public ClickstreamValidator(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonNode parse(byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new InvalidRecordException("payload is empty");
        }
        try {
            JsonNode node = objectMapper.readTree(payload);
            if (node == null || !node.isObject()) {
                throw new InvalidRecordException("payload must be a JSON object");
            }
            requireSupportedSchema(node);
            return node;
        } catch (InvalidRecordException e) {
            throw e;
        } catch (JacksonException e) {
            throw new InvalidRecordException("malformed JSON payload");
        }
    }

    static void requireSupportedSchema(JsonNode node) {
        if (node == null || !node.hasNonNull("schemaVersion")) {
            return;
        }
        JsonNode version = node.get("schemaVersion");
        if (!version.isIntegralNumber() || version.intValue() != SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidRecordException("unsupported schema version");
        }
    }
}
