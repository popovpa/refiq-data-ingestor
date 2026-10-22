package ru.refiq.strategy.audit;

import java.time.Instant;

public record AuditEvent(
        String eventId,
        int schemaVersion,
        String eventType,
        String action,
        Instant occurredAt,
        String accountId,
        String actorType,
        String actorId,
        String entityType,
        String entityId,
        String requestId,
        String correlationId,
        String ipAddress,
        String userAgent,
        String changedFieldsJson,
        String beforeJson,
        String afterJson,
        String metadataJson
) {
}
