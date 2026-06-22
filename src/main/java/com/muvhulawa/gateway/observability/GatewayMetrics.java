package com.muvhulawa.gateway.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/**
 * Operational signals for the bridge, exposed via Actuator / Prometheus: how many messages were
 * forwarded and replied (broken down by the processor's group status), how many were dead-lettered
 * and why, how often a transient downstream failure caused a redelivery, and the round-trip time to
 * the processor.
 */
@Component
public class GatewayMetrics {

    private static final String REPLIED = "gateway.messages.replied";
    private static final String DEAD_LETTERED = "gateway.messages.dead_lettered";
    private static final String RETRIED = "gateway.messages.transient_retries";

    private final MeterRegistry registry;
    private final Timer forwardTimer;

    public GatewayMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.forwardTimer = Timer.builder("gateway.processor.roundtrip")
                .description("Time to forward a pacs.008 to the processor and get the pacs.002 back")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
    }

    public void replied(String paymentStatus, boolean replay) {
        registry.counter(REPLIED, "status", paymentStatus, "replay", Boolean.toString(replay))
                .increment();
    }

    public void deadLettered(String kind) {
        registry.counter(DEAD_LETTERED, "kind", kind).increment();
    }

    public void transientRetry() {
        registry.counter(RETRIED).increment();
    }

    public <T> T timeForward(java.util.function.Supplier<T> forward) {
        return forwardTimer.record(forward);
    }
}
