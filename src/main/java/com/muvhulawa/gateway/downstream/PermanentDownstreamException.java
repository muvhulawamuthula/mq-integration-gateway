package com.muvhulawa.gateway.downstream;

/**
 * The processor rejected the request itself (4xx) — the message is structurally unacceptable to the
 * endpoint and will never succeed on retry. Redelivering it would just block the queue, so it is
 * routed straight to the dead-letter queue.
 *
 * <p>Note this is distinct from a business rejection: a {@code pacs.002 RJCT} comes back as a
 * normal 200 response and is a <em>successful</em> gateway outcome (the reply is published). Only a
 * transport-level 4xx reaches here.
 */
public class PermanentDownstreamException extends RuntimeException {
    public PermanentDownstreamException(String message, Throwable cause) {
        super(message, cause);
    }
}
