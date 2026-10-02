package ru.refiq.error;

import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaDeadLetterPublisher implements DeadLetterPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaDeadLetterPublisher(KafkaTemplate<byte[], byte[]> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(DeadLetter letter) {
        String topic = letter.sourceTopic() + ".dlq";
        try {
            byte[] body = objectMapper.writeValueAsBytes(letter);
            byte[] key = letter.keyBase64() == null ? null : Base64.getDecoder().decode(letter.keyBase64());
            kafkaTemplate.send(topic, key, body).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            log.warn(
                    "record sent to dlq topic={} sourceTopic={} partition={} offset={} category={} type={}",
                    topic,
                    letter.sourceTopic(),
                    letter.partition(),
                    letter.offset(),
                    letter.errorCategory(),
                    letter.ingestType()
            );
        } catch (TimeoutException e) {
            throw new RetryableIngestException("dlq publish timed out", e);
        } catch (ExecutionException e) {
            throw new RetryableIngestException("dlq publish failed", e.getCause() == null ? e : e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableIngestException("dlq publish interrupted", e);
        } catch (RetryableIngestException e) {
            throw e;
        } catch (Exception e) {
            throw new RetryableIngestException("dlq publish failed", e);
        }
    }
}
