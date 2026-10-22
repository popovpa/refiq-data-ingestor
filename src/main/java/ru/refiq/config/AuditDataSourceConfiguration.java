package ru.refiq.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@ConditionalOnAuditPipeline
public class AuditDataSourceConfiguration {

    @Bean(destroyMethod = "close")
    public HikariDataSource auditDataSource(DataIngestProperties properties) {
        DataIngestProperties.Postgres postgres = properties.getStorage().getPostgres();
        IngestStartupChecker.validatePostgres(properties, properties.getKafka().getMaxPollInterval());
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword() == null ? "" : postgres.getPassword());
        config.setPoolName("data-ingest-audit");
        config.setMaximumPoolSize(4);
        config.setMinimumIdle(0);
        config.setInitializationFailTimeout(-1);
        config.setConnectionTimeout(postgres.getRequestTimeout().toMillis());
        return new HikariDataSource(config);
    }

    @Bean
    public JdbcTemplate auditJdbcTemplate(DataSource auditDataSource, DataIngestProperties properties) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(auditDataSource);
        long seconds = properties.getStorage().getPostgres().getRequestTimeout().toSeconds();
        jdbcTemplate.setQueryTimeout((int) Math.max(1, seconds));
        return jdbcTemplate;
    }
}
