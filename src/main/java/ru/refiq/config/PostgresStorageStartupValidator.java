package ru.refiq.config;

import org.springframework.stereotype.Component;
import ru.refiq.error.FatalIngestException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Component
@ConditionalOnAuditPipeline
public class PostgresStorageStartupValidator implements IngestStorageStartupValidator {

    private final DataSource dataSource;

    public PostgresStorageStartupValidator(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public boolean supports(String type) {
        return "audit".equals(type);
    }

    @Override
    public void validate(DataIngestProperties properties) {
        DataIngestProperties.Postgres postgres = properties.getStorage().getPostgres();
        int timeoutSeconds = (int) Math.max(1, postgres.getRequestTimeout().toSeconds());
        try (Connection connection = dataSource.getConnection()) {
            connection.setReadOnly(true);
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(timeoutSeconds);
                try (ResultSet ping = statement.executeQuery("SELECT 1")) {
                    if (!ping.next()) {
                        throw new FatalIngestException("PostgreSQL storage is not available");
                    }
                }
            }
            try (PreparedStatement table = connection.prepareStatement(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?"
            )) {
                table.setQueryTimeout(timeoutSeconds);
                table.setString(1, postgres.getTable());
                try (ResultSet rows = table.executeQuery()) {
                    if (!rows.next()) {
                        throw new FatalIngestException("PostgreSQL table does not exist: " + postgres.getTable());
                    }
                }
            }
        } catch (FatalIngestException e) {
            throw e;
        } catch (SQLException e) {
            throw new FatalIngestException("PostgreSQL storage is not available", e);
        }
    }
}
