package io.github.moalrubaie.refunds;

/** An event that will fail every time, so retrying it is pointless. It goes straight to the DLT. */
public class PoisonEventException extends RuntimeException {

    public PoisonEventException(String message) {
        super(message);
    }
}
