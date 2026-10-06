package io.github.moalrubaie.refunds;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Bean
    NewTopic refundsTopic(AppProperties props) {
        return TopicBuilder.name(props.topic()).partitions(props.partitions()).build();
    }

    @Bean
    NewTopic deadLetterTopic(AppProperties props) {
        return TopicBuilder.name(props.deadLetterTopic())
                .partitions(props.partitions())
                .build();
    }

    /**
     * Retry, then park. A failed record is retried {@code app.attempts} times in total, then
     * published to the dead-letter topic with the error in its headers, and its offset is committed
     * so the partition moves on. Poison events skip the retries.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template, AppProperties props, Stats stats) {
        var deadLetter = new DeadLetterPublishingRecoverer(
                template, (record, ex) -> new TopicPartition(props.deadLetterTopic(), -1));
        var handler = new DefaultErrorHandler(
                (record, ex) -> {
                    deadLetter.accept(record, ex);
                    stats.deadLettered();
                    log.warn(
                            "Dead-lettered {}-{}@{}: {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            rootMessage(ex));
                },
                new FixedBackOff(props.retryBackoff().toMillis(), props.attempts() - 1L));
        handler.addNotRetryableExceptions(PoisonEventException.class);
        return handler;
    }

    private static String rootMessage(Throwable ex) {
        Throwable t = ex;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getMessage();
    }
}
