package ru.refiq.config;

import com.clickhouse.client.api.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.time.temporal.ChronoUnit;

@Configuration
public class IngestRuntimeConfiguration {

    @Bean
    public Clock ingestClock() {
        return Clock.systemUTC();
    }

    @Bean(destroyMethod = "close")
    public Client clickHouseInsertClient(DataIngestProperties properties) {
        DataIngestProperties.ClickHouse clickHouse = properties.getStorage().getClickhouse();
        return new Client.Builder()
                .addEndpoint(clickHouse.getUrl())
                .setUsername(clickHouse.getUsername())
                .setPassword(clickHouse.getPassword() == null ? "" : clickHouse.getPassword())
                .setDefaultDatabase(clickHouse.getDatabase())
                .setConnectTimeout(3, ChronoUnit.SECONDS)
                .setSocketTimeout(clickHouse.getRequestTimeout().toMillis(), ChronoUnit.MILLIS)
                .setExecutionTimeout(clickHouse.getRequestTimeout().toMillis(), ChronoUnit.MILLIS)
                .build();
    }
}
