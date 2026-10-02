package ru.refiq.strategy.clickstream;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.insert.InsertSettings;
import com.clickhouse.data.ClickHouseFormat;
import org.testng.annotations.Test;
import ru.refiq.support.TestIngest;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ClickstreamWriterTest {

    @Test
    public void writesTheWholeBatchAsOneRowBinaryInsert() {
        Client client = mock(Client.class);
        when(client.insert(anyString(), org.mockito.ArgumentMatchers.<List<String>>any(), any(InputStream.class), any(ClickHouseFormat.class), any(InsertSettings.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        ClickstreamWriter writer = new ClickstreamWriter(client, TestIngest.metrics(), TestIngest.properties());
        ClickstreamEventRecord first = new ClickstreamEventRecord();
        first.setEventId(11L);
        first.setClientEventId(9007199254740993L);
        ClickstreamEventRecord second = new ClickstreamEventRecord();
        second.setEventId(12L);

        writer.write(List.of(first, second));

        verify(client).insert(
                eq("`default`.`clickstream_events`"),
                eq(ClickstreamColumn.names()),
                any(InputStream.class),
                eq(ClickHouseFormat.RowBinary),
                any(InsertSettings.class)
        );
        verifyNoMoreInteractions(client);
    }

    @Test
    public void encodedBatchIsNonEmpty() throws Exception {
        Client client = mock(Client.class);
        when(client.insert(anyString(), org.mockito.ArgumentMatchers.<List<String>>any(), any(InputStream.class), any(ClickHouseFormat.class), any(InsertSettings.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        ClickstreamWriter writer = new ClickstreamWriter(client, TestIngest.metrics(), TestIngest.properties());
        writer.write(List.of(new ClickstreamEventRecord()));

        var stream = org.mockito.ArgumentCaptor.forClass(InputStream.class);
        verify(client).insert(any(), eq(ClickstreamColumn.names()), stream.capture(), eq(ClickHouseFormat.RowBinary), any());
        assertThat(stream.getValue().available(), greaterThan(0));
        assertThat(ClickHouseFormat.RowBinary.name(), is("RowBinary"));
    }
}
