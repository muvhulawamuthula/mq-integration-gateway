package com.muvhulawa.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

/**
 * All gateway-specific configuration, bound from {@code gateway.*}. Queue names and the processor
 * endpoint are externalised so the same artifact runs against an embedded broker locally and IBM MQ
 * in production without code changes.
 */
@ConfigurationProperties(prefix = "gateway")
public class GatewayProperties {

    @NestedConfigurationProperty
    private final Queues queues = new Queues();

    @NestedConfigurationProperty
    private final Processor processor = new Processor();

    /** JMS listener concurrency, e.g. "1-5" (min-max consumers). */
    private String concurrency = "1-5";

    public Queues getQueues() { return queues; }
    public Processor getProcessor() { return processor; }
    public String getConcurrency() { return concurrency; }
    public void setConcurrency(String concurrency) { this.concurrency = concurrency; }

    public static class Queues {
        /** Inbound queue carrying pacs.008 requests. */
        private String inbound = "PACS008.IN";
        /** Default reply queue for pacs.002 when the message carries no JMSReplyTo. */
        private String reply = "PACS002.OUT";
        /** Dead-letter queue for poison and exhausted messages. */
        private String deadLetter = "DLQ";

        public String getInbound() { return inbound; }
        public void setInbound(String inbound) { this.inbound = inbound; }
        public String getReply() { return reply; }
        public void setReply(String reply) { this.reply = reply; }
        public String getDeadLetter() { return deadLetter; }
        public void setDeadLetter(String deadLetter) { this.deadLetter = deadLetter; }
    }

    public static class Processor {
        /** Base URL of the ISO 20022 payment processor. */
        private String baseUrl = "http://localhost:8080";
        /** Path of the pacs.008 submission endpoint. */
        private String path = "/api/v1/payments/pacs008";

        public String getBaseUrl() { return baseUrl; }
        public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }
        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
    }
}
