package ru.refiq.config;

import org.testng.annotations.Test;
import ru.refiq.error.FatalIngestException;
import ru.refiq.support.TestIngest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

public class PostgresStorageStartupValidatorTest {

    @Test
    public void acceptsAReachableAuditTable() throws Exception {
        DataSource dataSource = dataSource(true, false);
        new PostgresStorageStartupValidator(dataSource).validate(TestIngest.auditProperties());
    }

    @Test
    public void missingAuditTableIsFatal() throws Exception {
        DataSource dataSource = dataSource(false, false);

        expectThrows(FatalIngestException.class, () -> new PostgresStorageStartupValidator(dataSource).validate(TestIngest.auditProperties()));
    }

    @Test
    public void unreachablePostgresIsFatal() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(new SQLException("connection refused", "08001"));

        expectThrows(FatalIngestException.class, () -> new PostgresStorageStartupValidator(dataSource).validate(TestIngest.auditProperties()));
    }

    @Test
    public void startupCheckDoesNotExecuteDdl() throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement table = mock(PreparedStatement.class);
        ResultSet ping = mock(ResultSet.class);
        ResultSet tables = mock(ResultSet.class);
        when(ping.next()).thenReturn(true);
        when(tables.next()).thenReturn(true);
        when(statement.executeQuery(anyString())).thenReturn(ping);
        when(table.executeQuery()).thenReturn(tables);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(table);
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);

        new PostgresStorageStartupValidator(dataSource).validate(TestIngest.auditProperties());

        verify(statement).executeQuery(startsWith("SELECT 1"));
        verify(connection).prepareStatement(startsWith("SELECT 1 FROM information_schema.tables"));
    }

    private static DataSource dataSource(boolean tableExists, boolean failPing) throws Exception {
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        PreparedStatement table = mock(PreparedStatement.class);
        ResultSet ping = mock(ResultSet.class);
        ResultSet tables = mock(ResultSet.class);
        when(ping.next()).thenReturn(!failPing);
        when(tables.next()).thenReturn(tableExists);
        when(statement.executeQuery(anyString())).thenReturn(ping);
        when(table.executeQuery()).thenReturn(tables);
        when(connection.createStatement()).thenReturn(statement);
        when(connection.prepareStatement(anyString())).thenReturn(table);
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(connection);
        return dataSource;
    }
}
