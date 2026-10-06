package io.github.moalrubaie.refunds;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

class RefundApprovedTest {

    private final JsonMapper mapper = JsonMapper.builder().build();

    @Test
    void parsesAValidEvent() {
        var event = RefundApproved.parse(
                "{\"eventId\":\"refund-1\",\"orderId\":\"order-9\",\"amountCents\":12000}", mapper);

        assertThat(event).isEqualTo(new RefundApproved("refund-1", "order-9", 12_000));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"eventId\":\"refund-1\",\"orderId\":", // cut off
                "not json",
                "null",
                "{\"orderId\":\"order-9\",\"amountCents\":100}", // no eventId
                "{\"eventId\":\"refund-1\",\"orderId\":\" \",\"amountCents\":100}", // blank orderId
                "{\"eventId\":\"refund-1\",\"orderId\":\"order-9\",\"amountCents\":-100}",
                "{\"eventId\":\"refund-1\",\"orderId\":\"order-9\",\"amountCents\":0}"
            })
    void rejectsPoisonAsNonRetryable(String json) {
        assertThatThrownBy(() -> RefundApproved.parse(json, mapper)).isInstanceOf(PoisonEventException.class);
    }
}
