package ru.refiq.strategy.clickstream;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.metrics.IngestMetrics;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionException;

@Component
public class ClickstreamWriter {

    private static final Logger log = LoggerFactory.getLogger(ClickstreamWriter.class);

    private final Client client;
    private final IngestMetrics metrics;
    private final String table;

    public ClickstreamWriter(Client client, IngestMetrics metrics, DataIngestProperties properties) {
        this.client = client;
        this.metrics = metrics;
        DataIngestProperties.ClickHouse clickHouse = properties.getStorage().getClickhouse();
        this.table = "`" + clickHouse.getDatabase() + "`.`" + clickHouse.getTable() + "`";
    }

    public void write(List<ClickstreamEventRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        byte[] body = ClickstreamRowBinary.encode(records);
        Timer.Sample storage = metrics.startStorageWrite();
        Timer.Sample insert = metrics.startClickHouseInsert();
        try {
            client.insert(
                    table,
                    ClickstreamColumn.names(),
                    new ByteArrayInputStream(body),
                    ClickHouseFormat.RowBinary,
                    new InsertSettings()
            ).join();
            metrics.clickhouseBatch(records.size());
            metrics.eventsInserted(records.size());
        } catch (CompletionException e) {
            fail(records.size(), e.getCause() == null ? e : e.getCause());
        } catch (RuntimeException e) {
            fail(records.size(), e);
        } finally {
            metrics.stopStorageWrite(storage);
            metrics.stopClickHouseInsert(insert);
        }
        log.debug("clickhouse batch insert table={} records={}", table, records.size());
    }

    private void fail(int records, Throwable error) {
        metrics.storageError();
        metrics.eventsFailed(records);
        log.error(
                "clickhouse batch insert failed table={} records={} reason={}",
                table,
                records,
                summarize(error)
        );
        throw translate(error);
    }

    private static RuntimeException translate(Throwable error) {
        if (error instanceof FatalIngestException fatal) {
            return fatal;
        }
        if (error instanceof RetryableIngestException retryable) {
            return retryable;
        }
        String message = summarize(error).toLowerCase();
        if (message.contains("code: 16")
                || message.contains("code: 41")
                || message.contains("code: 47")
                || message.contains("code: 53")
                || message.contains("code: 60")
                || message.contains("code: 62")
                || message.contains("unknown identifier")
                || message.contains("no such column")
                || message.contains("doesn't exist")
                || message.contains("does not exist")
                || message.contains("type mismatch")) {
            return new FatalIngestException("clickhouse insert rejected", error);
        }
        if (error instanceof IOException || error instanceof FatalIngestException) {
            return new RetryableIngestException("clickhouse insert failed", error);
        }
        return new RetryableIngestException("clickhouse insert failed", error);
    }

    private static String summarize(Throwable error) {
        String message = error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
        String oneLine = message.replace('\n', ' ').replace('\r', ' ').trim();
        return oneLine.length() <= 180 ? oneLine : oneLine.substring(0, 180);
    }
}
