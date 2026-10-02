package ru.refiq.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Validated
@ConfigurationProperties(prefix = "data-ingest")
public class DataIngestProperties {

    @Valid
    @NotNull
    private Source source = new Source();

    @Valid
    @NotNull
    private Kafka kafka = new Kafka();

    @Valid
    @NotNull
    private Batch batch = new Batch();

    @Valid
    @NotNull
    private Retry retry = new Retry();

    @Valid
    @NotNull
    private Monitoring monitoring = new Monitoring();

    @Valid
    @NotNull
    private Storage storage = new Storage();

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Monitoring getMonitoring() {
        return monitoring;
    }

    public void setMonitoring(Monitoring monitoring) {
        this.monitoring = monitoring;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public static class Source {

        @NotBlank
        private String topic;

        @NotBlank
        private String type;

        @NotBlank
        private String groupId;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }
    }

    public static class Kafka {

        @NotBlank
        private String bootstrapServers;

        @NotNull
        private Duration maxPollInterval = Duration.ofMinutes(5);

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public Duration getMaxPollInterval() {
            return maxPollInterval;
        }

        public void setMaxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
        }
    }

    public static class Batch {

        @Positive
        private int maxRecords;

        @Positive
        private long maxBytes;

        @NotNull
        private Duration maxWait;

        @NotNull
        private Duration pollTimeout;

        public int getMaxRecords() {
            return maxRecords;
        }

        public void setMaxRecords(int maxRecords) {
            this.maxRecords = maxRecords;
        }

        public long getMaxBytes() {
            return maxBytes;
        }

        public void setMaxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        public Duration getMaxWait() {
            return maxWait;
        }

        public void setMaxWait(Duration maxWait) {
            this.maxWait = maxWait;
        }

        public Duration getPollTimeout() {
            return pollTimeout;
        }

        public void setPollTimeout(Duration pollTimeout) {
            this.pollTimeout = pollTimeout;
        }
    }

    public static class Retry {

        @NotNull
        private Duration initialBackoff = Duration.ofMillis(200);

        @NotNull
        private Duration maxBackoff = Duration.ofSeconds(5);

        public Duration getInitialBackoff() {
            return initialBackoff;
        }

        public void setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
        }

        public Duration getMaxBackoff() {
            return maxBackoff;
        }

        public void setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
        }
    }

    public static class Monitoring {

        @Min(1)
        @Max(65535)
        private int port;

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class Storage {

        private ClickHouse clickhouse = new ClickHouse();

        private Postgres postgres;

        public ClickHouse getClickhouse() {
            return clickhouse;
        }

        public void setClickhouse(ClickHouse clickhouse) {
            this.clickhouse = clickhouse;
        }

        public Postgres getPostgres() {
            return postgres;
        }

        public void setPostgres(Postgres postgres) {
            this.postgres = postgres;
        }
    }

    public static class ClickHouse {

        @NotBlank
        private String url;

        @NotBlank
        private String database;

        @NotBlank
        private String username;

        @NotNull
        private String password = "";

        @NotBlank
        private String table;

        @NotNull
        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }
    }

    public static class Postgres {

        private String url;

        private String username;

        private String password = "";

        private String table = "audit_logs";

        private Duration requestTimeout = Duration.ofSeconds(30);

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public Duration getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(Duration requestTimeout) {
            this.requestTimeout = requestTimeout;
        }
    }
}
