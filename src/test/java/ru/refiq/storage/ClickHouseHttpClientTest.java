package ru.refiq.storage;

import com.sun.net.httpserver.HttpServer;
import org.testng.annotations.Test;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.support.TestIngest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.testng.Assert.expectThrows;

public class ClickHouseHttpClientTest {

    @Test
    public void postsABatchAsOneRequest() throws IOException {
        AtomicInteger calls = new AtomicInteger();
        AtomicReference<String> body = new AtomicReference<>();
        HttpServer server = server(200, "{\"ok\":1}", calls, body);
        try {
            client(server).insertJsonEachRow(
                    "INSERT INTO `default`.`clickstream_events` FORMAT JSONEachRow",
                    "{\"client_event_id\":\"one\"}\n{\"client_event_id\":\"two\"}\n"
            );
        } finally {
            server.stop(0);
        }

        assertThat(calls.get(), is(1));
        assertThat(body.get(), containsString("\"client_event_id\":\"one\""));
        assertThat(body.get(), containsString("\"client_event_id\":\"two\""));
    }

    @Test
    public void metadataChecksAreReadOnly() throws IOException {
        List<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = server(200, "1\n", new AtomicInteger(), new AtomicReference<>(), queries);
        try {
            ClickHouseHttpClient clickHouse = client(server);
            clickHouse.ping();
            assertThat(clickHouse.databaseExists("default"), is(true));
            assertThat(clickHouse.tableExists("default", "clickstream_events"), is(true));
        } finally {
            server.stop(0);
        }

        assertThat(queries, is(List.of(
                "SELECT 1",
                "SELECT count() FROM system.databases WHERE name = 'default'",
                "SELECT count() FROM system.tables WHERE database = 'default' AND name = 'clickstream_events'"
        )));
        for (String query : queries) {
            assertThat(query.toUpperCase(), not(containsString("CREATE")));
            assertThat(query.toUpperCase(), not(containsString("ALTER")));
            assertThat(query.toUpperCase(), not(containsString("DROP")));
        }
    }

    @Test
    public void serverErrorIsRetryableAndClientErrorIsFatal() throws IOException {
        HttpServer retryable = server(503, "unavailable", new AtomicInteger(), new AtomicReference<>());
        try {
            expectThrows(RetryableIngestException.class, () -> client(retryable).ping());
        } finally {
            retryable.stop(0);
        }

        HttpServer fatal = server(400, "Code: 47. Unknown identifier", new AtomicInteger(), new AtomicReference<>());
        try {
            expectThrows(FatalIngestException.class, () -> client(fatal).ping());
        } finally {
            fatal.stop(0);
        }
    }

    private static ClickHouseHttpClient client(HttpServer server) {
        DataIngestProperties properties = TestIngest.properties();
        properties.getStorage().getClickhouse().setUrl("http://127.0.0.1:" + server.getAddress().getPort());
        return new ClickHouseHttpClient(properties);
    }

    private static HttpServer server(int status, String response, AtomicInteger calls, AtomicReference<String> body) throws IOException {
        return server(status, response, calls, body, new ArrayList<>());
    }

    private static HttpServer server(
            int status,
            String response,
            AtomicInteger calls,
            AtomicReference<String> body,
            List<String> queries
    ) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            calls.incrementAndGet();
            String payloadText = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            body.set(payloadText);
            String query = exchange.getRequestURI().getRawQuery();
            if (query != null && query.contains("query=")) {
                queries.add(java.net.URLDecoder.decode(query.substring(query.indexOf("query=") + 6), StandardCharsets.UTF_8));
            } else {
                queries.add(payloadText);
            }
            byte[] payload = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.start();
        return server;
    }
}
