package com.muvhulawa.gateway.downstream;

import org.springframework.http.ResponseEntity;

/**
 * The processor's answer to a pacs.008 submission: the pacs.002 document plus the status metadata
 * it exposes as headers. A response is produced for every understood message — including business
 * rejections (RJCT) and idempotent replays — which is exactly why the gateway can rely on the
 * processor's idempotency to make at-least-once JMS delivery safe.
 */
public record ProcessorResponse(String paymentStatus, boolean idempotentReplay, String pacs002Xml) {

    public static ProcessorResponse from(ResponseEntity<String> entity) {
        String status = entity.getHeaders().getFirst("X-Payment-Status");
        boolean replay = Boolean.parseBoolean(entity.getHeaders().getFirst("X-Idempotent-Replay"));
        return new ProcessorResponse(status == null ? "UNKNOWN" : status, replay, entity.getBody());
    }
}
