package ru.refiq.strategy.clickstream;

import tools.jackson.databind.JsonNode;

/**
 * Maps a browser client event id onto Int64.
 * The current SDK still sends {@code [a-z0-9]{20}}. That token does not fit in a long
 * without losing precision, so it is not hashed and not replaced with a generated id.
 */
public interface ClientEventIdMapper {

    long map(JsonNode value);
}
