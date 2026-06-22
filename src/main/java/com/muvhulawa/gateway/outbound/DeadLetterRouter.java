package com.muvhulawa.gateway.outbound;

import com.muvhulawa.gateway.config.GatewayProperties;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;

/**
 * Routes a message the gateway cannot or should not process to the dead-letter queue, tagged with
 * why and where it came from. This is the explicit, application-level path used for <em>permanent</em>
 * failures (a 4xx from the processor, or a non-text payload) — where retrying is pointless and
 * leaving the message on the inbound queue would block every message behind it (head-of-line
 * blocking). Transient failures take the other path: the broker redelivers and only dead-letters
 * after its backout threshold.
 */
@Component
public class DeadLetterRouter {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterRouter.class);

    private final JmsTemplate jmsTemplate;
    private final String deadLetterQueue;

    public DeadLetterRouter(JmsTemplate jmsTemplate, GatewayProperties properties) {
        this.jmsTemplate = jmsTemplate;
        this.deadLetterQueue = properties.getQueues().getDeadLetter();
    }

    public void route(Message original, String reason) {
        String body = bodyOf(original);
        String originalId = idOf(original);
        // JMS property names must be valid Java identifiers — no hyphens.
        jmsTemplate.convertAndSend(deadLetterQueue, body, message -> {
            message.setStringProperty("gateway_dlq_reason", reason);
            message.setStringProperty("gateway_original_msg_id", originalId);
            message.setLongProperty("gateway_failed_at", System.currentTimeMillis());
            return message;
        });
        log.warn("Dead-lettered message origId={} reason={}", originalId, reason);
    }

    private static String bodyOf(Message message) {
        try {
            return message instanceof TextMessage text ? text.getText() : "<non-text message body>";
        } catch (JMSException e) {
            return "<unreadable message body>";
        }
    }

    private static String idOf(Message message) {
        try {
            return message.getJMSMessageID();
        } catch (JMSException e) {
            return "<unknown>";
        }
    }
}
