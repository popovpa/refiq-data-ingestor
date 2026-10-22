package ru.refiq.strategy.audit;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnAuditPipeline;
import ru.refiq.error.InvalidRecordException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Component
@ConditionalOnAuditPipeline
public class AuditEventValidator {

    static final int SUPPORTED_SCHEMA_VERSION = 1;

    private final ObjectMapper objectMapper;

    public AuditEventValidator(ObjectMapper objectMapper) {
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
            return node;
        } catch (InvalidRecordException e) {
            throw e;
        } catch (JacksonException e) {
            throw new InvalidRecordException("malformed JSON payload");
        }
    }
}
