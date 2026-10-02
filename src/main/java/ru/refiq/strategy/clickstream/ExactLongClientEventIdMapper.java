package ru.refiq.strategy.clickstream;

import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.error.InvalidRecordException;
import tools.jackson.databind.JsonNode;

import java.util.regex.Pattern;

@Component
@ConditionalOnClickstreamPipeline
public class ExactLongClientEventIdMapper implements ClientEventIdMapper {

    private static final Pattern LEGACY = Pattern.compile("^[a-z0-9]{20}$");

    @Override
    public long map(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return 0L;
        }
        if (value.isIntegralNumber()) {
            return ClickstreamNumbers.requireLong(value, "clientEventId", Long.MIN_VALUE, Long.MAX_VALUE);
        }
        if (!value.isTextual()) {
            throw new InvalidRecordException("clientEventId is missing or invalid");
        }
        String text = value.asText("").trim();
        if (text.isEmpty()) {
            return 0L;
        }
        if (ClickstreamNumbers.isCanonicalLong(text)) {
            return Long.parseLong(text);
        }
        if (LEGACY.matcher(text).matches()) {
            return 0L;
        }
        throw new InvalidRecordException("clientEventId is missing or invalid");
    }
}
