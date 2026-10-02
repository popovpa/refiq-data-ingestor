package ru.refiq.config;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import ru.refiq.batch.IngestBatch;
import ru.refiq.consumer.IngestConsumerWorker;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.metrics.BufferGauge;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.storage.ClickHouseClient;
import ru.refiq.strategy.IngestStrategy;
import ru.refiq.strategy.IngestStrategyRegistry;
import ru.refiq.support.TestIngest;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;

public class InfrastructureStartupTest {

    private KafkaTopicProbe kafkaTopicProbe;
    private ClickHouseClient clickHouseClient;
    private InfrastructureReadiness readiness;
    private IngestStartupChecker checker;

    @BeforeMethod
    public void setUp() {
        kafkaTopicProbe = mock(KafkaTopicProbe.class);
        clickHouseClient = mock(ClickHouseClient.class);
        readiness = mock(InfrastructureReadiness.class);
        when(kafkaTopicProbe.topicExists("clickstream-events")).thenReturn(true);
        when(clickHouseClient.databaseExists("default")).thenReturn(true);
        when(clickHouseClient.tableExists("default", "clickstream_events")).thenReturn(true);
        checker = new IngestStartupChecker(
                TestIngest.properties(),
                new IngestStrategyRegistry(List.of(strategy("clickstream"))),
                kafkaTopicProbe,
                readiness,
                List.of(new ClickHouseStorageStartupValidator(clickHouseClient))
        );
    }

    @Test
    public void existingInfrastructureMarksReadyBeforeConsumerCanStart() {
        checker.afterPropertiesSet();

        InOrder order = inOrder(kafkaTopicProbe, clickHouseClient, readiness);
        order.verify(kafkaTopicProbe).topicExists("clickstream-events");
        order.verify(clickHouseClient).ping();
        order.verify(clickHouseClient).databaseExists("default");
        order.verify(clickHouseClient).tableExists("default", "clickstream_events");
        order.verify(readiness).markReady();
        verifyNoMoreInteractions(clickHouseClient);
    }

    @Test
    public void missingTableFailsStartup() {
        when(clickHouseClient.tableExists("default", "clickstream_events")).thenReturn(false);

        FatalIngestException error = expectThrows(FatalIngestException.class, checker::afterPropertiesSet);

        assertThat(error.getMessage(), containsString("table does not exist"));
        verify(readiness, never()).markReady();
    }

    @Test
    public void missingDatabaseFailsStartup() {
        when(clickHouseClient.databaseExists("default")).thenReturn(false);

        FatalIngestException error = expectThrows(FatalIngestException.class, checker::afterPropertiesSet);

        assertThat(error.getMessage(), containsString("database does not exist"));
        verify(clickHouseClient, never()).tableExists(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.anyString());
        verify(readiness, never()).markReady();
    }

    @Test
    public void unavailableClickHouseFailsStartup() {
        doThrow(new RetryableIngestException("connection refused")).when(clickHouseClient).ping();

        FatalIngestException error = expectThrows(FatalIngestException.class, checker::afterPropertiesSet);

        assertThat(error.getMessage(), containsString("not available"));
        verify(readiness, never()).markReady();
    }

    @Test
    public void existingKafkaTopicPassesValidation() {
        checker.afterPropertiesSet();

        verify(kafkaTopicProbe).topicExists("clickstream-events");
        verify(readiness).markReady();
    }

    @Test
    public void missingKafkaTopicFailsStartup() {
        when(kafkaTopicProbe.topicExists("clickstream-events")).thenReturn(false);

        FatalIngestException error = expectThrows(FatalIngestException.class, checker::afterPropertiesSet);

        assertThat(error.getMessage(), containsString("topic does not exist"));
        verify(clickHouseClient, never()).ping();
        verify(readiness, never()).markReady();
    }

    @Test
    public void validationDoesNotIssueDdl() {
        checker.afterPropertiesSet();

        verify(clickHouseClient).ping();
        verify(clickHouseClient).databaseExists("default");
        verify(clickHouseClient).tableExists("default", "clickstream_events");
        verifyNoMoreInteractions(clickHouseClient);
    }

    @Test
    public void consumerDoesNotStartBeforeInfrastructureValidation() {
        InfrastructureReadiness gate = new InfrastructureReadiness();
        org.springframework.kafka.core.ConsumerFactory<byte[], byte[]> consumerFactory = mock(org.springframework.kafka.core.ConsumerFactory.class);
        IngestConsumerWorker worker = new IngestConsumerWorker(
                consumerFactory,
                TestIngest.properties(),
                new IngestStrategyRegistry(List.of(strategy("clickstream"))),
                mock(DeadLetterPublisher.class),
                mock(IngestMetrics.class),
                new BufferGauge(),
                gate,
                mock(org.springframework.context.ConfigurableApplicationContext.class)
        );

        expectThrows(FatalIngestException.class, worker::start);

        verify(consumerFactory, never()).createConsumer();
        assertThat(worker.isRunning(), org.hamcrest.Matchers.is(false));
    }

    private static IngestStrategy strategy(String type) {
        return new IngestStrategy() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void process(IngestBatch batch) {
            }
        };
    }
}
