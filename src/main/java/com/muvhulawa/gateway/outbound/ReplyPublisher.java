package com.muvhulawa.gateway.outbound;

import com.muvhulawa.gateway.config.GatewayProperties;
import com.muvhulawa.gateway.downstream.ProcessorResponse;
import jakarta.jms.Destination;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes the pacs.002 reply. It honours the inbound {@code JMSReplyTo} if the sender set one
 * (point-to-point request/reply), otherwise falls back to the configured reply queue. The reply is
 * correlated to the request via {@code JMSCorrelationID} so the sender can match it, and the
 * processor's status headers are surfaced as JMS properties for cheap routing/monitoring without
 * parsing the XML.
 */
@Component
public class ReplyPublisher {

    private static final Logger log = LoggerFactory.getLogger(ReplyPublisher.class);

    private final JmsTemplate jmsTemplate;
    private final String defaultReplyQueue;

    public ReplyPublisher(JmsTemplate jmsTemplate, GatewayProperties properties) {
        this.jmsTemplate = jmsTemplate;
        this.defaultReplyQueue = properties.getQueues().getReply();
    }

    public void publish(ProcessorResponse response, String correlationId, Destination replyTo) {
        // JMS property names must be valid Java identifiers (no hyphens), so we cannot reuse the
        // processor's HTTP header names verbatim.
        MessageDecorator decorator = message -> {
            message.setJMSCorrelationID(correlationId);
            message.setStringProperty("payment_status", response.paymentStatus());
            message.setBooleanProperty("idempotent_replay", response.idempotentReplay());
        };

        if (replyTo != null) {
            jmsTemplate.convertAndSend(replyTo, response.pacs002Xml(), m -> decorate(m, decorator));
        } else {
            jmsTemplate.convertAndSend(defaultReplyQueue, response.pacs002Xml(),
                    m -> decorate(m, decorator));
        }
        log.info("Published pacs.002 reply ({}) corrId={}", response.paymentStatus(), correlationId);
    }

    private static Message decorate(Message message, MessageDecorator decorator) throws JMSException {
        decorator.apply(message);
        return message;
    }

    @FunctionalInterface
    private interface MessageDecorator {
        void apply(Message message) throws JMSException;
    }
}
