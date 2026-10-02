package ru.refiq.strategy.clickstream;

import ru.refiq.error.InvalidRecordException;
import tools.jackson.databind.JsonNode;

import java.util.List;
import java.util.Set;

final class ClickstreamJsonBinder {

    private static final Set<ClickstreamColumn> LENIENT_IDS = Set.of(
            ClickstreamColumn.VISITOR_ID,
            ClickstreamColumn.SESSION_ID,
            ClickstreamColumn.PAGE_VIEW_ID,
            ClickstreamColumn.PREVIOUS_PAGE_VIEW_ID,
            ClickstreamColumn.BATCH_ID,
            ClickstreamColumn.REQUEST_ID
    );

    void bind(JsonNode node, ClickstreamEventRecord record) {
        if (node == null || !node.isObject()) {
            return;
        }
        for (ClickstreamColumn column : ClickstreamColumn.values()) {
            if (column.skipBrowserBind()) {
                continue;
            }
            JsonNode value = find(node, column);
            if (value == null) {
                continue;
            }
            apply(column, value, record);
        }
    }

    private void apply(ClickstreamColumn column, JsonNode value, ClickstreamEventRecord record) {
        if (column.textual()) {
            String text = text(column, value);
            if (text.length() > column.maxChars()) {
                throw new InvalidRecordException(column.columnName() + " is too long");
            }
            record.set(column, text);
            return;
        }
        if (column.floating()) {
            if (!value.isNumber()) {
                throw new InvalidRecordException(column.columnName() + " is invalid");
            }
            double number = value.doubleValue();
            if (!Double.isFinite(number)) {
                throw new InvalidRecordException(column.columnName() + " is invalid");
            }
            record.set(column, (float) number);
            return;
        }
        Long number = ClickstreamNumbers.readLong(value, column.columnName());
        if (number == null) {
            if (LENIENT_IDS.contains(column)) {
                return;
            }
            throw new InvalidRecordException(column.columnName() + " is invalid");
        }
        if (number < column.minNumber() || number > column.maxNumber()) {
            throw new InvalidRecordException(column.columnName() + " is invalid");
        }
        record.set(column, number);
    }

    private static String text(ClickstreamColumn column, JsonNode value) {
        if (value.isArray()) {
            StringBuilder joined = new StringBuilder();
            for (JsonNode item : value) {
                if (item == null || !item.isValueNode()) {
                    throw new InvalidRecordException(column.columnName() + " is invalid");
                }
                if (joined.length() > 0) {
                    joined.append(',');
                }
                joined.append(item.asText(""));
            }
            return joined.toString();
        }
        if (!value.isValueNode()) {
            throw new InvalidRecordException(column.columnName() + " is invalid");
        }
        return value.asText("").trim();
    }

    private static JsonNode find(JsonNode node, ClickstreamColumn column) {
        JsonNode flat = firstValue(node, column.aliases());
        if (flat != null) {
            return flat;
        }
        if (column.jsonGroup() == null || column.groupMember() == null) {
            return null;
        }
        JsonNode group = groupNode(node, column.jsonGroup());
        if (group == null || !group.isObject()) {
            return null;
        }
        String member = column.groupMember();
        return firstValue(group, List.of(member, camel(member)));
    }

    private static JsonNode groupNode(JsonNode node, String group) {
        JsonNode direct = node.get(group);
        if (direct != null && direct.isObject()) {
            return direct;
        }
        String snake = snake(group);
        JsonNode snakeNode = node.get(snake);
        if (snakeNode != null && snakeNode.isObject()) {
            return snakeNode;
        }
        if (group.endsWith("s")) {
            JsonNode singular = node.get(group.substring(0, group.length() - 1));
            if (singular != null && singular.isObject()) {
                return singular;
            }
        }
        return null;
    }

    private static JsonNode firstValue(JsonNode node, List<String> names) {
        for (String name : names) {
            JsonNode value = node.get(name);
            if (value != null && !value.isNull() && !value.isContainer()) {
                return value;
            }
            if (value != null && value.isArray()) {
                return value;
            }
        }
        return null;
    }

    private static String camel(String snake) {
        StringBuilder builder = new StringBuilder();
        boolean upper = false;
        for (int i = 0; i < snake.length(); i++) {
            char ch = snake.charAt(i);
            if (ch == '_') {
                upper = true;
                continue;
            }
            builder.append(upper ? Character.toUpperCase(ch) : ch);
            upper = false;
        }
        return builder.toString();
    }

    private static String snake(String camel) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char ch = camel.charAt(i);
            if (Character.isUpperCase(ch)) {
                builder.append('_').append(Character.toLowerCase(ch));
            } else {
                builder.append(ch);
            }
        }
        return builder.toString();
    }
}
