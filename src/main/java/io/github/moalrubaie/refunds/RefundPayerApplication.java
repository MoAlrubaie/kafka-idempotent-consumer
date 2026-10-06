package io.github.moalrubaie.refunds;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * A service that reads approved refunds from Kafka and pays them out.
 *
 * <p>Kafka delivers at least once, so the same refund can arrive more than once. Which {@link
 * io.github.moalrubaie.refunds.sink.RefundSink} is active (set by {@code app.sink}) decides whether
 * a duplicate turns into a second payment.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RefundPayerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RefundPayerApplication.class, args);
    }
}
