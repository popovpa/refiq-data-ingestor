package ru.refiq.strategy.clickstream;

import ru.refiq.error.InvalidRecordException;
import tools.jackson.databind.JsonNode;

final class ClickstreamNumbers {

    private ClickstreamNumbers() {
    }

    static boolean isCanonicalLong(String text) {
        if (text.isEmpty() || !text.matches("-?\\d+")) {
            return false;
        }
        try {
            return Long.toString(Long.parseLong(text)).equals(text);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    static long requireLong(JsonNode node, String field, long min, long max) {
        Long value = readLong(node, field);
        if (value == null || value < min || value > max) {
            throw new InvalidRecordException(field + " is invalid");
        }
        return value;
    }

    /**
     * @return the long, or null when the node is not numeric
     */
    static Long readLong(JsonNode node, String field) {
        if (node.isIntegralNumber()) {
            if (!node.canConvertToLong()) {
                throw new InvalidRecordException(field + " is invalid");
            }
            return node.longValue();
        }
        if (node.isTextual()) {
            String text = node.asText("").trim();
            if (!text.matches("-?\\d+")) {
                return null;
            }
            if (!isCanonicalLong(text)) {
                throw new InvalidRecordException(field + " is invalid");
            }
            return Long.parseLong(text);
        }
        if (node.isBoolean()) {
            return node.booleanValue() ? 1L : 0L;
        }
        return null;
    }
}
