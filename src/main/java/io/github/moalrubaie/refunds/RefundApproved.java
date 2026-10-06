package io.github.moalrubaie.refunds;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * An approved refund. {@code eventId} identifies this refund for its whole life: every retry,
 * resend and replay carries the same ID, which is what makes it usable as an idempotency key.
 */
public record RefundApproved(String eventId, String orderId, long amountCents) {

    /** Parses and validates a record value, or throws {@link PoisonEventException}. */
    public static RefundApproved parse(String json, JsonMapper mapper) {
        RefundApproved event;
        try {
            event = mapper.readValue(json, RefundApproved.class);
        } catch (JacksonException e) {
            throw new PoisonEventException("not a valid refund event: " + e.getOriginalMessage());
        }
        if (event == null || isBlank(event.eventId()) || isBlank(event.orderId())) {
            throw new PoisonEventException("refund event without an eventId or orderId");
        }
        if (event.amountCents() <= 0) {
            throw new PoisonEventException("refund amount must be positive, was " + event.amountCents());
        }
        return event;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
