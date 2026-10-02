package ru.refiq.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.refiq.config.ConditionalOnClickstreamPipeline;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Component
@ConditionalOnClickstreamPipeline
public class ClickHouseHttpClient implements ClickHouseClient {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseHttpClient.class);

    private final HttpClient httpClient;
    private final URI baseUri;
    private final String database;
    private final String username;
    private final String password;
    private final Duration requestTimeout;

    @Autowired
    public ClickHouseHttpClient(DataIngestProperties properties) {
        this(properties, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build());
    }

    ClickHouseHttpClient(DataIngestProperties properties, HttpClient httpClient) {
        DataIngestProperties.ClickHouse clickHouse = properties.getStorage().getClickhouse();
        this.baseUri = URI.create(stripTrailingSlash(clickHouse.getUrl()));
        this.database = clickHouse.getDatabase();
        this.username = clickHouse.getUsername();
        this.password = clickHouse.getPassword() == null ? "" : clickHouse.getPassword();
        this.requestTimeout = clickHouse.getRequestTimeout();
        this.httpClient = httpClient;
    }

    @Override
    public void ping() {
        query("SELECT 1", "system");
    }

    @Override
    public boolean databaseExists(String database) {
        String sql = "SELECT count() FROM system.databases WHERE name = '" + database + "'";
        return count(query(sql, "system")) > 0;
    }

    @Override
    public boolean tableExists(String database, String table) {
        String sql = "SELECT count() FROM system.tables WHERE database = '" + database + "' AND name = '" + table + "'";
        return count(query(sql, "system")) > 0;
    }

    @Override
    public void insertJsonEachRow(String insertSql, String jsonEachRowBody) {
        send(insertSql, jsonEachRowBody, true, database);
    }

    private String query(String sql, String databaseContext) {
        return send(sql, sql, false, databaseContext);
    }

    private String send(String sql, String body, boolean queryInUrl, String databaseContext) {
        StringBuilder uri = new StringBuilder(baseUri.toString())
                .append("/?database=")
                .append(encode(databaseContext))
                .append("&async_insert=0");
        if (queryInUrl) {
            uri.append("&query=").append(encode(sql));
        }
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(uri.toString()))
                .timeout(requestTimeout)
                .header("X-ClickHouse-User", username)
                .header("X-ClickHouse-Key", password)
                .POST(HttpRequest.BodyPublishers.ofString(queryInUrl ? body : sql, StandardCharsets.UTF_8));
        try {
            HttpResponse<String> response = httpClient.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                return response.body();
            }
            throw forStatus(response.statusCode(), response.body());
        } catch (HttpTimeoutException e) {
            throw new RetryableIngestException("clickhouse request timed out", e);
        } catch (IOException e) {
            throw new RetryableIngestException("clickhouse request failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableIngestException("clickhouse request interrupted", e);
        }
    }

    private static long count(String body) {
        String value = body == null ? "" : body.trim();
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new FatalIngestException("unexpected clickhouse metadata response");
        }
    }

    private RuntimeException forStatus(int status, String body) {
        String detail = summarize(body);
        if (status == 429 || status >= 500) {
            log.error("clickhouse request failed status={} detail={}", status, detail);
            return new RetryableIngestException("clickhouse request failed with status " + status);
        }
        log.error("clickhouse request rejected status={} detail={}", status, detail);
        return new FatalIngestException("clickhouse request rejected with status " + status + ": " + detail);
    }

    private static String summarize(String body) {
        if (body == null || body.isBlank()) {
            return "";
        }
        String oneLine = body.replace('\n', ' ').replace('\r', ' ').trim();
        return oneLine.length() <= 180 ? oneLine : oneLine.substring(0, 180);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String stripTrailingSlash(String url) {
        if (url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }
}
