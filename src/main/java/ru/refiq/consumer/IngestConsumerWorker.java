package ru.refiq.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import ru.refiq.batch.BatchAccumulator;
import ru.refiq.batch.IngestRecord;
import ru.refiq.config.DataIngestProperties;
import ru.refiq.config.InfrastructureReadiness;
import ru.refiq.error.CommitAbandonedException;
import ru.refiq.error.DeadLetterPublisher;
import ru.refiq.error.FatalIngestException;
import ru.refiq.error.RetryableIngestException;
import ru.refiq.metrics.BufferGauge;
import ru.refiq.metrics.IngestMetrics;
import ru.refiq.strategy.IngestStrategy;
import ru.refiq.strategy.IngestStrategyRegistry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class IngestConsumerWorker implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(IngestConsumerWorker.class);
    private static final Duration SHUTDOWN_JOIN = Duration.ofSeconds(25);
    private static final Duration COMMIT_TIMEOUT = Duration.ofSeconds(10);

    private final ConsumerFactory<byte[], byte[]> consumerFactory;
    private final DataIngestProperties properties;
    private final IngestStrategyRegistry registry;
    private final DeadLetterPublisher deadLetterPublisher;
    private final IngestMetrics metrics;
    private final BufferGauge buffer;
    private final InfrastructureReadiness readiness;
    private final ConfigurableApplicationContext applicationContext;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Thread thread;
    private volatile Consumer<byte[], byte[]> consumer;
    private volatile boolean fatal;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    public IngestConsumerWorker(
            ConsumerFactory<byte[], byte[]> ingestConsumerFactory,
            DataIngestProperties properties,
            IngestStrategyRegistry registry,
            DeadLetterPublisher deadLetterPublisher,
            IngestMetrics metrics,
            BufferGauge buffer,
            InfrastructureReadiness readiness,
            ConfigurableApplicationContext applicationContext
    ) {
        this.consumerFactory = ingestConsumerFactory;
        this.properties = properties;
        this.registry = registry;
        this.deadLetterPublisher = deadLetterPublisher;
        this.metrics = metrics;
        this.buffer = buffer;
        this.readiness = readiness;
        this.applicationContext = applicationContext;
    }

    @Override
    public void start() {
        if (!readiness.isReady()) {
            throw new FatalIngestException("infrastructure validation has not completed");
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::runLoop, "data-ingest-consumer");
        thread.start();
    }

    @Override
    public void stop() {
        stop(() -> {
        });
    }

    @Override
    public void stop(Runnable callback) {
        if (running.compareAndSet(true, false)) {
            Consumer<byte[], byte[]> current = consumer;
            if (current != null) {
                try {
                    current.wakeup();
                } catch (RuntimeException e) {
                    log.debug("consumer wakeup during shutdown failed");
                }
            }
            Thread currentThread = thread;
            if (currentThread != null) {
                try {
                    currentThread.join(SHUTDOWN_JOIN.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        callback.run();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1024;
    }

    private void runLoop() {
        IngestStrategy strategy = registry.require(properties.getSource().getType());
        IngestSession session = new IngestSession(
                new BatchAccumulator(
                        properties.getBatch().getMaxRecords(),
                        properties.getBatch().getMaxBytes(),
                        properties.getBatch().getMaxWait()
                ),
                strategy,
                this::commit,
                deadLetterPublisher,
                metrics,
                new RetryPolicy(properties.getRetry().getInitialBackoff(), properties.getRetry().getMaxBackoff()),
                properties.getSource().getType(),
                buffer
        );
        Duration pollTimeout = properties.getBatch().getPollTimeout();
        log.info(
                "data-ingest consumer started topic={} type={} groupId={}",
                properties.getSource().getTopic(),
                properties.getSource().getType(),
                properties.getSource().getGroupId()
        );
        Consumer<byte[], byte[]> kafkaConsumer = consumerFactory.createConsumer();
        this.consumer = kafkaConsumer;
        try {
            kafkaConsumer.subscribe(List.of(properties.getSource().getTopic()), new RebalanceHandler(session));
            while (running.get() && !fatal) {
                if (session.hasPending()) {
                    pause(kafkaConsumer);
                    ConsumerRecords<byte[], byte[]> heartbeat = poll(kafkaConsumer, pollTimeout);
                    if (heartbeat == null) {
                        break;
                    }
                    rewind(kafkaConsumer, heartbeat);
                    long now = System.nanoTime();
                    if (session.retryDue(now)) {
                        try {
                            session.retry(now);
                        } catch (RetryableIngestException e) {
                            log.warn("batch retry still failing type={}", properties.getSource().getType());
                        } catch (FatalIngestException e) {
                            fail(e);
                            break;
                        }
                    }
                    if (!session.hasPending()) {
                        resume(kafkaConsumer);
                    }
                    continue;
                }
                long now = System.nanoTime();
                ConsumerRecords<byte[], byte[]> polled = poll(kafkaConsumer, pollTimeout);
                if (polled == null) {
                    break;
                }
                if (polled.isEmpty()) {
                    try {
                        session.onIdle(System.nanoTime());
                    } catch (RetryableIngestException e) {
                        log.warn("time-based flush will be retried type={}", properties.getSource().getType());
                    } catch (FatalIngestException e) {
                        fail(e);
                        break;
                    }
                    continue;
                }
                List<IngestRecord> records = IngestRecord.from(polled, now);
                try {
                    int consumed = session.offer(records, System.nanoTime());
                    if (consumed < records.size()) {
                        seekFrom(kafkaConsumer, records, consumed);
                    }
                } catch (RetryableIngestException e) {
                    seekFrom(kafkaConsumer, records, session.resumeFrom());
                    log.warn("batch paused for retry type={}", properties.getSource().getType());
                } catch (FatalIngestException e) {
                    fail(e);
                    break;
                }
            }
            if (!fatal) {
                shutdown(session);
            }
        } catch (WakeupException e) {
            if (!fatal) {
                shutdown(session);
            }
        } catch (FatalIngestException e) {
            fail(e);
        } finally {
            try {
                kafkaConsumer.close(Duration.ofSeconds(5));
            } catch (RuntimeException e) {
                log.warn("kafka consumer close failed type={}", properties.getSource().getType());
            }
            this.consumer = null;
            if (fatal) {
                requestExit();
            }
        }
    }

    private void shutdown(IngestSession session) {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        try {
            session.shutdown(System.nanoTime());
            log.info("data-ingest consumer stopped type={}", properties.getSource().getType());
        } catch (WakeupException e) {
            session.shutdown(System.nanoTime());
            log.info("data-ingest consumer stopped type={}", properties.getSource().getType());
        } catch (FatalIngestException e) {
            fail(e);
        }
    }

    private void fail(FatalIngestException error) {
        fatal = true;
        running.set(false);
        log.error("fatal ingest error, terminating type={}", properties.getSource().getType(), error);
    }

    private void requestExit() {
        Thread exit = new Thread(() -> {
            int code = SpringApplication.exit(applicationContext, () -> 1);
            System.exit(code);
        }, "data-ingest-fatal-exit");
        exit.setDaemon(false);
        exit.start();
    }

    private ConsumerRecords<byte[], byte[]> poll(Consumer<byte[], byte[]> kafkaConsumer, Duration pollTimeout) {
        try {
            return kafkaConsumer.poll(pollTimeout);
        } catch (WakeupException e) {
            if (!running.get() || fatal) {
                return null;
            }
            throw e;
        } catch (RetriableException e) {
            metrics.retry();
            log.warn("kafka poll failed type={}", properties.getSource().getType());
            return ConsumerRecords.empty();
        }
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) {
            return;
        }
        Consumer<byte[], byte[]> current = consumer;
        if (current == null) {
            throw new RetryableIngestException("kafka consumer is not active");
        }
        try {
            current.commitSync(offsets, COMMIT_TIMEOUT);
        } catch (CommitFailedException e) {
            throw new CommitAbandonedException("kafka commit abandoned after rebalance", e);
        } catch (WakeupException e) {
            throw e;
        } catch (RetriableException e) {
            metrics.commitError();
            throw new RetryableIngestException("kafka commit failed", e);
        } catch (RuntimeException e) {
            metrics.commitError();
            throw new RetryableIngestException("kafka commit failed", e);
        }
    }

    private static void pause(Consumer<byte[], byte[]> kafkaConsumer) {
        var assigned = kafkaConsumer.assignment();
        if (!assigned.isEmpty()) {
            kafkaConsumer.pause(assigned);
        }
    }

    private static void resume(Consumer<byte[], byte[]> kafkaConsumer) {
        var paused = kafkaConsumer.paused();
        if (!paused.isEmpty()) {
            kafkaConsumer.resume(paused);
        }
    }

    private static void rewind(Consumer<byte[], byte[]> kafkaConsumer, ConsumerRecords<byte[], byte[]> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        Map<TopicPartition, Long> earliest = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            earliest.merge(new TopicPartition(record.topic(), record.partition()), record.offset(), Math::min);
        }
        earliest.forEach(kafkaConsumer::seek);
    }

    static void seekFrom(Consumer<byte[], byte[]> kafkaConsumer, List<IngestRecord> records, int fromIndex) {
        Map<TopicPartition, Long> earliest = new HashMap<>();
        for (int i = Math.max(fromIndex, 0); i < records.size(); i++) {
            IngestRecord record = records.get(i);
            earliest.merge(record.topicPartition(), record.offset(), Math::min);
        }
        earliest.forEach(kafkaConsumer::seek);
    }

    private final class RebalanceHandler implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

        private final IngestSession session;

        private RebalanceHandler(IngestSession session) {
            this.session = session;
        }

        @Override
        public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
            log.info("kafka partitions revoked type={} partitions={}", properties.getSource().getType(), partitions);
            try {
                session.onRevoke(java.util.Set.copyOf(partitions), System.nanoTime());
            } catch (FatalIngestException e) {
                fail(e);
            }
        }

        @Override
        public void onPartitionsAssigned(java.util.Collection<TopicPartition> partitions) {
            log.info("kafka partitions assigned type={} partitions={}", properties.getSource().getType(), partitions);
        }
    }
}
