package com.muvhulawa.gateway.downstream;

/**
 * The processor could not be reached or returned a server error (5xx / connection failure).
 * Retrying later may succeed, so the inbound message is rolled back and redelivered rather than
 * dead-lettered. Only after the broker's redelivery threshold is exhausted does it become poison.
 */
public class TransientDownstreamException extends RuntimeException {
    public TransientDownstreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
