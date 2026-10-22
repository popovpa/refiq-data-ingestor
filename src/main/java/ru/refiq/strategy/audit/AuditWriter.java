package ru.refiq.strategy.audit;

import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnAuditPipeline;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.metrics.IngestMetrics;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

@Component
@ConditionalOnAuditPipeline
public class AuditWriter {

    private static final Logger log = LoggerFactory.getLogger(AuditWriter.class);

    private final JdbcTemplate jdbcTemplate;
    private final IngestMetrics metrics;
    private final String sql;

    public AuditWriter(JdbcTemplate jdbcTemplate, IngestMetrics metrics, DataIngestProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.metrics = metrics;
        String table = properties.getStorage().getPostgres().getTable();
        this.sql = "INSERT INTO " + table + " ("
                + "event_id, schema_version, event_type, action, occurred_at, "
                + "account_id, actor_type, actor_id, entity_type, entity_id, "
                + "request_id, correlation_id, ip_address, user_agent, "
                + "changed_fields, before_data, after_data, metadata"
                + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb),CAST(? AS jsonb),CAST(? AS jsonb),CAST(? AS jsonb)) "
                + "ON CONFLICT (event_id) DO NOTHING";
    }

    public void write(List<AuditEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        Timer.Sample sample = metrics.startStorageWrite();
        try {
            jdbcTemplate.execute((Connection connection) -> persist(connection, events));
            metrics.eventsInserted(events.size());
        } catch (RetryableIngestException | FatalIngestException e) {
            metrics.storageError();
            throw e;
        } catch (RuntimeException e) {
            metrics.storageError();
            throw classify(e);
        } finally {
            metrics.stopStorageWrite(sample);
        }
        log.debug("postgres batch insert table=audit_logs records={}", events.size());
    }

    private Void persist(Connection connection, List<AuditEvent> events) {
        boolean originalAutoCommit = true;
        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (AuditEvent event : events) {
                    bind(statement, event);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
            connection.commit();
            return null;
        } catch (SQLException e) {
            rollback(connection);
            throw classify(e);
        } finally {
            restoreAutoCommit(connection, originalAutoCommit);
        }
    }

    private static void bind(PreparedStatement statement, AuditEvent event) throws SQLException {
        statement.setString(1, event.eventId());
        statement.setInt(2, event.schemaVersion());
        statement.setString(3, event.eventType());
        statement.setString(4, event.action());
        statement.setTimestamp(5, Timestamp.from(event.occurredAt() == null ? Instant.EPOCH : event.occurredAt()));
        nullable(statement, 6, event.accountId());
        statement.setString(7, event.actorType());
        nullable(statement, 8, event.actorId());
        statement.setString(9, event.entityType());
        statement.setString(10, event.entityId());
        nullable(statement, 11, event.requestId());
        nullable(statement, 12, event.correlationId());
        nullable(statement, 13, event.ipAddress());
        nullable(statement, 14, event.userAgent());
        nullable(statement, 15, event.changedFieldsJson());
        nullable(statement, 16, event.beforeJson());
        nullable(statement, 17, event.afterJson());
        nullable(statement, 18, event.metadataJson());
    }

    private static void nullable(PreparedStatement statement, int index, String value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignored) {
            // the original failure is reported to the caller
        }
    }

    private static void restoreAutoCommit(Connection connection, boolean original) {
        try {
            connection.setAutoCommit(original);
        } catch (SQLException ignored) {
            // connection will be closed by the pool
        }
    }

    private static RuntimeException classify(Throwable error) {
        SQLException sql = findSql(error);
        if (sql != null && permanent(sql)) {
            return new FatalIngestException("postgres insert rejected", sql);
        }
        return new RetryableIngestException("postgres insert failed", error);
    }

    private static boolean permanent(SQLException error) {
        String state = error.getSQLState() == null ? "" : error.getSQLState();
        return state.equals("42P01") || state.equals("42703") || state.equals("42804") || state.startsWith("42") || state.startsWith("23");
    }

    private static SQLException findSql(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof SQLException sql) {
                return sql;
            }
            current = current.getCause();
        }
        return null;
    }
}
