package ru.refiq.strategy.audit;

import org.testng.annotations.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class AuditEventMapperTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AuditEventValidator validator = new AuditEventValidator(objectMapper);
    private final AuditEventMapper mapper = new AuditEventMapper(objectMapper);

    @Test
    public void mapsTheVersionOneContract() {
        AuditEvent event = mapper.map(validator.parse(payload().getBytes()));

        assertThat(event.eventId(), is("evt_01"));
        assertThat(event.schemaVersion(), is(1));
        assertThat(event.eventType(), is("OFFER_UPDATED"));
        assertThat(event.action(), is("UPDATE"));
        assertThat(event.occurredAt(), is(Instant.parse("2026-10-03T12:30:15.123Z")));
        assertThat(event.accountId(), is("acc_123"));
        assertThat(event.actorType(), is("USER"));
        assertThat(event.actorId(), is("usr_456"));
        assertThat(event.entityType(), is("OFFER"));
        assertThat(event.entityId(), is("off_789"));
        assertThat(event.requestId(), is("req_1"));
        assertThat(event.correlationId(), is("corr_1"));
        assertThat(event.ipAddress(), is("127.0.0.1"));
        assertThat(event.userAgent(), is("agent"));
        assertThat(event.changedFieldsJson(), is("{\"holdPeriodDays\":{\"before\":7,\"after\":14}}"));
        assertThat(event.beforeJson(), is("{\"holdPeriodDays\":7}"));
        assertThat(event.afterJson(), is("{\"holdPeriodDays\":14}"));
        assertThat(event.metadataJson(), is("{}"));
    }

    @Test
    public void systemEventMayOmitAccountAndActorId() {
        String json = """
                {"schemaVersion":1,"eventId":"evt_sys","eventType":"SYSTEM_TICK","occurredAt":"2026-10-03T12:30:15Z","actor":{"type":"SYSTEM"},"entity":{"type":"INTEGRATION","id":"int_1"},"action":"SYSTEM_ACTION"}
                """;

        AuditEvent event = mapper.map(validator.parse(json.getBytes()));

        assertThat(event.accountId(), nullValue());
        assertThat(event.actorId(), nullValue());
        assertThat(event.changedFieldsJson(), nullValue());
        assertThat(event.entityType(), is("INTEGRATION"));
    }

    private static String payload() {
        return """
                {
                  "schemaVersion": 1,
                  "eventId": "evt_01",
                  "eventType": "OFFER_UPDATED",
                  "occurredAt": "2026-10-03T12:30:15.123Z",
                  "accountId": "acc_123",
                  "actor": {"type": "USER", "id": "usr_456"},
                  "entity": {"type": "OFFER", "id": "off_789"},
                  "action": "UPDATE",
                  "requestId": "req_1",
                  "correlationId": "corr_1",
                  "ipAddress": "127.0.0.1",
                  "userAgent": "agent",
                  "changes": {"holdPeriodDays": {"before": 7, "after": 14}},
                  "before": {"holdPeriodDays": 7},
                  "after": {"holdPeriodDays": 14},
                  "metadata": {}
                }
                """;
    }
}
