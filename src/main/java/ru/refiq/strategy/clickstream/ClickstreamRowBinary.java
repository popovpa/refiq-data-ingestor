package ru.refiq.strategy.clickstream;

import com.clickhouse.client.api.data_formats.RowBinaryFormatWriter;
import com.clickhouse.client.api.metadata.TableSchema;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseFormat;
import ru.refiq.error.FatalIngestException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Inet6Address;
import java.util.ArrayList;
import java.util.List;

final class ClickstreamRowBinary {

    private static final TableSchema SCHEMA = schema();

    private ClickstreamRowBinary() {
    }

    static byte[] encode(List<ClickstreamEventRecord> records) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            RowBinaryFormatWriter writer = new RowBinaryFormatWriter(out, SCHEMA, ClickHouseFormat.RowBinary);
            for (ClickstreamEventRecord record : records) {
                for (ClickstreamColumn column : ClickstreamColumn.values()) {
                    writer.setValue(column.columnName(), wire(column, record.get(column)));
                }
                writer.commitRow();
            }
        } catch (IOException | RuntimeException e) {
            throw new FatalIngestException("clickstream row encoding failed", e);
        }
        return out.toByteArray();
    }

    private static Object wire(ClickstreamColumn column, Object value) {
        if (column.internetAddress()) {
            return address(value);
        }
        if (column.floating()) {
            return value instanceof Float ? value : ((Number) value).floatValue();
        }
        return value;
    }

    private static Inet6Address address(Object value) {
        return ClickstreamAddresses.inet6(value == null ? "::" : value.toString());
    }

    private static TableSchema schema() {
        List<ClickHouseColumn> columns = new ArrayList<>();
        for (ClickstreamColumn column : ClickstreamColumn.values()) {
            columns.add(ClickHouseColumn.of(column.columnName(), column.wireType()));
        }
        return new TableSchema(columns);
    }
}
