package io.github.moalrubaie.refunds;

import io.github.moalrubaie.refunds.sink.RefundSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Hands each delivery to the active sink. Auto-commit is off, and the container commits this
 * record's offset only after the method returns (ack-mode: record). An exception means "not done":
 * the error handler retries the record and dead-letters it after the last attempt.
 */
@Component
public class RefundListener {

    private final RefundSink sink;
    private final JsonMapper mapper;
    private final Stats stats;

    public RefundListener(RefundSink sink, JsonMapper mapper, Stats stats) {
        this.sink = sink;
        this.mapper = mapper;
        this.stats = stats;
    }

    @KafkaListener(id = "refund-payer", idIsGroup = false, topics = "${app.topic}", concurrency = "${app.partitions}")
    public void onRefund(ConsumerRecord<String, String> record) {
        stats.delivered();
        sink.pay(RefundApproved.parse(record.value(), mapper));
    }
}
