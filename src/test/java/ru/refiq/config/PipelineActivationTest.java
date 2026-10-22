package ru.refiq.config;

import com.clickhouse.client.api.Client;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.annotations.Test;
import ru.refiq.storage.ClickHouseHttpClient;
import ru.refiq.strategy.audit.AuditIngestStrategy;
import ru.refiq.strategy.audit.AuditWriter;
import ru.refiq.strategy.clickstream.ClickstreamIngestStrategy;
import ru.refiq.strategy.clickstream.ClickstreamMapper;
import ru.refiq.strategy.clickstream.ClickstreamValidator;
import ru.refiq.strategy.clickstream.ClickstreamWriter;
import ru.refiq.support.TestIngest;

import javax.sql.DataSource;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class PipelineActivationTest {

    private static DataIngestProperties activeProperties;

    @Test
    public void clickstreamActivatesOnlyClickHousePipeline() {
        try (AnnotationConfigApplicationContext components = components("clickstream");
             AnnotationConfigApplicationContext storage = storage("clickstream", TestIngest.properties())) {
            assertThat(components.containsBeanDefinition("clickstreamIngestStrategy"), is(true));
            assertThat(components.containsBeanDefinition("clickstreamValidator"), is(true));
            assertThat(components.containsBeanDefinition("clickstreamMapper"), is(true));
            assertThat(components.containsBeanDefinition("clickstreamWriter"), is(true));
            assertThat(components.containsBeanDefinition("clickHouseHttpClient"), is(true));
            assertThat(components.containsBeanDefinition("clickHouseStorageStartupValidator"), is(true));
            assertThat(components.containsBeanDefinition("auditIngestStrategy"), is(false));
            assertThat(components.containsBeanDefinition("auditWriter"), is(false));
            assertThat(components.containsBeanDefinition("postgresStorageStartupValidator"), is(false));
            assertThat(components.getBeanFactory().getBeanNamesForType(AuditIngestStrategy.class).length, is(0));
            assertThat(components.getBeanFactory().getBeanNamesForType(ClickstreamIngestStrategy.class).length, is(1));
            assertThat(components.getBeanFactory().getBeanNamesForType(ClickHouseHttpClient.class).length, is(1));
            assertThat(storage.containsBeanDefinition("clickHouseInsertClient"), is(true));
            assertThat(storage.containsBeanDefinition("auditDataSource"), is(false));
            assertThat(storage.containsBeanDefinition("auditJdbcTemplate"), is(false));
            assertThat(storage.getBeanFactory().getBeanNamesForType(Client.class).length, is(1));
            assertThat(storage.getBeanFactory().getBeanNamesForType(DataSource.class).length, is(0));
            assertThat(storage.getBeanFactory().getBeanNamesForType(JdbcTemplate.class).length, is(0));
        }
    }

    @Test
    public void auditActivatesOnlyPostgresPipeline() {
        try (AnnotationConfigApplicationContext components = components("audit");
             AnnotationConfigApplicationContext storage = storage("audit", TestIngest.auditProperties())) {
            assertThat(components.containsBeanDefinition("auditIngestStrategy"), is(true));
            assertThat(components.containsBeanDefinition("auditWriter"), is(true));
            assertThat(components.containsBeanDefinition("postgresStorageStartupValidator"), is(true));
            assertThat(components.containsBeanDefinition("clickstreamIngestStrategy"), is(false));
            assertThat(components.containsBeanDefinition("clickstreamValidator"), is(false));
            assertThat(components.containsBeanDefinition("clickstreamMapper"), is(false));
            assertThat(components.containsBeanDefinition("clickstreamWriter"), is(false));
            assertThat(components.containsBeanDefinition("clickHouseHttpClient"), is(false));
            assertThat(components.containsBeanDefinition("clickHouseStorageStartupValidator"), is(false));
            assertThat(components.getBeanFactory().getBeanNamesForType(ClickstreamIngestStrategy.class).length, is(0));
            assertThat(components.getBeanFactory().getBeanNamesForType(ClickHouseHttpClient.class).length, is(0));
            assertThat(components.getBeanFactory().getBeanNamesForType(AuditIngestStrategy.class).length, is(1));
            assertThat(components.getBeanFactory().getBeanNamesForType(AuditWriter.class).length, is(1));
            assertThat(storage.containsBeanDefinition("auditDataSource"), is(true));
            assertThat(storage.containsBeanDefinition("auditJdbcTemplate"), is(true));
            assertThat(storage.containsBeanDefinition("clickHouseInsertClient"), is(false));
            assertThat(storage.getBeanFactory().getBeanNamesForType(Client.class).length, is(0));
            assertThat(storage.getBeanFactory().getBeanNamesForType(DataSource.class).length, is(1));
            assertThat(storage.getBeanFactory().getBeanNamesForType(JdbcTemplate.class).length, is(1));
        }
    }

    @Test
    public void clickstreamConfigurationDoesNotRequirePostgres() {
        IngestStartupChecker.validate(TestIngest.properties());
    }

    @Test
    public void auditConfigurationDoesNotRequireClickHouse() {
        IngestStartupChecker.validate(TestIngest.auditProperties());
    }

    private static AnnotationConfigApplicationContext components(String type) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                "pipeline",
                Map.of("data-ingest.source.type", type)
        ));
        context.register(
                ClickHouseHttpClient.class,
                ClickHouseStorageStartupValidator.class,
                PostgresStorageStartupValidator.class,
                ClickstreamValidator.class,
                ClickstreamMapper.class,
                ClickstreamWriter.class,
                ClickstreamIngestStrategy.class,
                AuditWriter.class,
                AuditIngestStrategy.class
        );
        return context;
    }

    private static AnnotationConfigApplicationContext storage(String type, DataIngestProperties properties) {
        activeProperties = properties;
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource(
                "pipeline",
                Map.of("data-ingest.source.type", type)
        ));
        context.register(PropertiesSource.class, IngestRuntimeConfiguration.class, AuditDataSourceConfiguration.class);
        context.refresh();
        return context;
    }

    @Configuration
    static class PropertiesSource {
        @Bean
        DataIngestProperties dataIngestProperties() {
            return activeProperties;
        }
    }
}
