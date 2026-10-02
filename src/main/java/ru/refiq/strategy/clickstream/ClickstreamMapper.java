package ru.refiq.strategy.clickstream;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.error.InvalidRecordException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

@Component
@ConditionalOnClickstreamPipeline
public class ClickstreamMapper {

    private static final Pattern SCRIPT_ID = Pattern.compile("^[a-z0-9]{5}$");
    private static final Pattern RQCID = Pattern.compile("^[a-z0-9]{12}$");
    private static final Pattern EVENT_TYPE = Pattern.compile("^[a-z0-9_]{1,64}$");

    private final ObjectMapper objectMapper;
    private final ClientEventIdMapper clientEventIdMapper;
    private final ClickstreamJsonBinder binder = new ClickstreamJsonBinder();

    public ClickstreamMapper(ObjectMapper objectMapper, ClientEventIdMapper clientEventIdMapper) {
        this.objectMapper = objectMapper;
        this.clientEventIdMapper = clientEventIdMapper;
    }

    public ClickstreamMapping map(JsonNode node) {
        if (node.has("events")) {
            return mapBatch(node);
        }
        try {
            return ClickstreamMapping.of(List.of(mapEvent(node, node)), List.of());
        } catch (InvalidRecordException e) {
            return ClickstreamMapping.malformed(e.getMessage());
        }
    }

    private ClickstreamMapping mapBatch(JsonNode envelope) {
        JsonNode events = envelope.get("events");
        if (events == null || !events.isArray() || events.isEmpty()) {
            return ClickstreamMapping.malformed("events array is missing or empty");
        }
        List<ClickstreamEventRecord> valid = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        for (JsonNode event : events) {
            if (event == null || !event.isObject()) {
                errors.add("event must be a JSON object");
                continue;
            }
            try {
                valid.add(mapEvent(envelope, event));
            } catch (InvalidRecordException e) {
                errors.add(e.getMessage());
            }
        }
        return ClickstreamMapping.of(valid, errors);
    }

    private ClickstreamEventRecord mapEvent(JsonNode envelope, JsonNode event) {
        ClickstreamValidator.requireSupportedSchema(event);
        ClickstreamEventRecord record = new ClickstreamEventRecord();
        if (envelope != event) {
            binder.bind(envelope, record);
        }
        binder.bind(event, record);
        record.setClientEventId(clientEventIdMapper.map(clientEventId(event, envelope)));
        applyLegacySiteKey(event, envelope, record);
        record.setSiteKey(record.siteKey().toLowerCase(Locale.ROOT));
        applyRqcid(event, envelope, record);
        applyCustomPayload(event, record);
        validate(record);
        return record;
    }

    private void validate(ClickstreamEventRecord record) {
        if (record.schemaVersion() != ClickstreamValidator.SUPPORTED_SCHEMA_VERSION) {
            throw new InvalidRecordException("unsupported schema version");
        }
        if (!EVENT_TYPE.matcher(record.eventType()).matches()) {
            throw new InvalidRecordException("event type is missing or invalid");
        }
        if (record.occurredTs() < 0) {
            throw new InvalidRecordException("occurred_ts is invalid");
        }
        if (record.sequence() < 0) {
            throw new InvalidRecordException("sequence is invalid");
        }
        if (!SCRIPT_ID.matcher(record.siteKey()).matches()) {
            throw new InvalidRecordException("script_id is missing or invalid");
        }
        if (!record.rqcid().isEmpty() && !RQCID.matcher(record.rqcid()).matches()) {
            throw new InvalidRecordException("rqcid is invalid");
        }
    }

    private void applyLegacySiteKey(JsonNode event, JsonNode envelope, ClickstreamEventRecord record) {
        if (!record.siteKey().isEmpty()) {
            return;
        }
        String legacy = text(event, "site_id");
        if (legacy.isEmpty() && envelope != event) {
            legacy = text(envelope, "site_id");
        }
        if (SCRIPT_ID.matcher(legacy).matches()) {
            record.setSiteKey(legacy);
        }
    }

    private void applyRqcid(JsonNode event, JsonNode envelope, ClickstreamEventRecord record) {
        if (!record.rqcid().isEmpty()) {
            return;
        }
        JsonNode payload = event.get("payload");
        if (payload != null && payload.isObject()) {
            JsonNode attribution = payload.get("attribution");
            if (attribution != null && attribution.isObject()) {
                String attributed = text(attribution, "rqcid");
                if (!attributed.isEmpty()) {
                    record.setRqcid(attributed);
                    return;
                }
            }
        }
        if (envelope != event) {
            String fromEnvelope = text(envelope, "rqcid");
            if (!fromEnvelope.isEmpty()) {
                record.setRqcid(fromEnvelope);
            }
        }
    }

    private void applyCustomPayload(JsonNode event, ClickstreamEventRecord record) {
        if (record.customPayloadRaw().isEmpty()) {
            JsonNode payload = event.get("payload");
            if (payload != null && !payload.isNull()) {
                if (!payload.isObject()) {
                    throw new InvalidRecordException("payload must be a JSON object");
                }
                try {
                    record.setCustomPayloadRaw(objectMapper.writeValueAsString(payload));
                } catch (JacksonException e) {
                    throw new InvalidRecordException("payload cannot be encoded");
                }
            }
        }
        if (record.customPayloadRaw().length() > ClickstreamColumn.CUSTOM_PAYLOAD_RAW.maxChars()) {
            throw new InvalidRecordException("custom_payload_raw is too long");
        }
        int bytes = record.customPayloadRaw().getBytes(StandardCharsets.UTF_8).length;
        if (bytes > ClickstreamColumn.CUSTOM_PAYLOAD_BYTES.maxNumber()) {
            throw new InvalidRecordException("custom_payload_raw is too long");
        }
        record.setCustomPayloadBytes(bytes);
    }

    private static JsonNode clientEventId(JsonNode event, JsonNode envelope) {
        JsonNode value = first(event, "clientEventId", "client_event_id");
        if (value != null) {
            return value;
        }
        if (envelope != event) {
            return first(envelope, "clientEventId", "client_event_id");
        }
        return null;
    }

    private static JsonNode first(JsonNode node, String... names) {
        if (node == null) {
            return null;
        }
        for (String name : names) {
            JsonNode value = node.get(name);
            if (value != null && !value.isNull()) {
                return value;
            }
        }
        return null;
    }

    private static String text(JsonNode node, String field) {
        if (node == null) {
            return "";
        }
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isValueNode()) {
            return "";
        }
        return value.asText("").trim();
    }
}
