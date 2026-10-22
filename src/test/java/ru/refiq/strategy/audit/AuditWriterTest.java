package ru.refiq.strategy.audit;

import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.Test;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.support.TestIngest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

public class AuditWriterTest {

    @Test
    public void insertsABatchIdempotentlyInOneTransaction() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        Connection connection = connection(statement, null);
        JdbcTemplate jdbcTemplate = jdbcTemplate(connection);
        AuditWriter writer = writer(jdbcTemplate);

        writer.write(List.of(event("evt_01"), event("evt_01")));

        verify(statement, org.mockito.Mockito.times(2)).addBatch();
        verify(statement).executeBatch();
        verify(connection).commit();
        verify(connection).setAutoCommit(false);
        assertThat(capturedSql(connection), containsString("ON CONFLICT (event_id) DO NOTHING"));
        assertThat(capturedSql(connection), containsString("INSERT INTO audit_logs"));
    }

    @Test
    public void temporaryDatabaseErrorIsRetryable() throws Exception {
        Connection connection = connection(mock(PreparedStatement.class), new SQLException("connection refused", "08001"));
        AuditWriter writer = writer(jdbcTemplate(connection));

        expectThrows(RetryableIngestException.class, () -> writer.write(List.of(event("evt_01"))));
        verify(connection).rollback();
    }

    @Test
    public void missingTableIsFatal() throws Exception {
        Connection connection = connection(mock(PreparedStatement.class), new SQLException("undefined_table", "42P01"));
        AuditWriter writer = writer(jdbcTemplate(connection));

        expectThrows(FatalIngestException.class, () -> writer.write(List.of(event("evt_01"))));
    }

    private static AuditWriter writer(JdbcTemplate jdbcTemplate) {
        return new AuditWriter(jdbcTemplate, TestIngest.metrics(), TestIngest.auditProperties());
    }

    private static JdbcTemplate jdbcTemplate(Connection connection) {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.execute(any(ConnectionCallback.class))).thenAnswer(invocation -> {
            ConnectionCallback<?> callback = invocation.getArgument(0);
            return callback.doInConnection(connection);
        });
        return jdbcTemplate;
    }

    private static Connection connection(PreparedStatement statement, SQLException failure) throws SQLException {
        Connection connection = mock(Connection.class);
        when(connection.getAutoCommit()).thenReturn(true);
        if (failure == null) {
            when(connection.prepareStatement(any())).thenReturn(statement);
        } else {
            when(connection.prepareStatement(any())).thenThrow(failure);
        }
        return connection;
    }

    private static String capturedSql(Connection connection) throws SQLException {
        org.mockito.ArgumentCaptor<String> sql = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(connection).prepareStatement(sql.capture());
        return sql.getValue();
    }

    private static AuditEvent event(String eventId) {
        return new AuditEvent(
                eventId, 1, "OFFER_UPDATED", "UPDATE", Instant.parse("2026-10-03T12:30:15.123Z"),
                "acc_123", "USER", "usr_456", "OFFER", "off_789",
                "req_1", "corr_1", "127.0.0.1", "agent",
                "{\"holdPeriodDays\":{\"before\":7,\"after\":14}}",
                "{\"holdPeriodDays\":7}",
                "{\"holdPeriodDays\":14}",
                "{}"
        );
    }
}
