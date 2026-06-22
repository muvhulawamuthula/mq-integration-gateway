package com.muvhulawa.gateway.inbound;

import com.muvhulawa.gateway.downstream.PermanentDownstreamException;
import com.muvhulawa.gateway.downstream.ProcessorClient;
import com.muvhulawa.gateway.downstream.ProcessorResponse;
import com.muvhulawa.gateway.downstream.TransientDownstreamException;
import com.muvhulawa.gateway.observability.GatewayMetrics;
import com.muvhulawa.gateway.outbound.DeadLetterRouter;
import com.muvhulawa.gateway.outbound.ReplyPublisher;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

/**
 * Consumes pacs.008 messages from the inbound MQ queue and bridges them to the payment processor.
 *
 * <p>The listener runs in a <strong>transacted</strong> session, which is what makes the three
 * failure modes behave correctly:
 *
 * <ul>
 *   <li><b>Success</b> (incl. a business RJCT pacs.002): publish the reply and let the transaction
 *       commit, consuming the message.</li>
 *   <li><b>Transient</b> downstream failure (processor down / 5xx): rethrow, so the transaction
 *       rolls back and the broker redelivers. The processor's MsgId idempotency makes this safe —
 *       a redelivered pacs.008 is deduplicated rather than paid twice. After the broker's backout
 *       threshold, it dead-letters the message itself.</li>
 *   <li><b>Permanent</b> failure (4xx, or a non-text payload): route to the DLQ explicitly and let
 *       the transaction commit. Retrying cannot help, and leaving it on the queue would block
 *       everything behind it.</li>
 * </ul>
 */
@Component
public class Pacs008Listener {

    private static final Logger log = LoggerFactory.getLogger(Pacs008Listener.class);

    private final ProcessorClient processorClient;
    private final ReplyPublisher replyPublisher;
    private final DeadLetterRouter deadLetterRouter;
    private final GatewayMetrics metrics;

    public Pacs008Listener(ProcessorClient processorClient,
                           ReplyPublisher replyPublisher,
                           DeadLetterRouter deadLetterRouter,
                           GatewayMetrics metrics) {
        this.processorClient = processorClient;
        this.replyPublisher = replyPublisher;
        this.deadLetterRouter = deadLetterRouter;
        this.metrics = metrics;
    }

    @JmsListener(destination = "${gateway.queues.inbound}", containerFactory = "gatewayListenerFactory")
    public void onMessage(Message message) throws JMSException {
        MDC.put("jmsMsgId", safeId(message));
        try {
            if (!(message instanceof TextMessage text)) {
                metrics.deadLettered("non_text");
                deadLetterRouter.route(message, "non-text JMS message; expected a pacs.008 TextMessage");
                return;
            }

            String pacs008 = text.getText();
            try {
                ProcessorResponse response = metrics.timeForward(() -> processorClient.submit(pacs008));
                replyPublisher.publish(response, correlationId(message), message.getJMSReplyTo());
                metrics.replied(response.paymentStatus(), response.idempotentReplay());
            } catch (TransientDownstreamException e) {
                // Roll back -> broker redelivers; do NOT dead-letter here.
                metrics.transientRetry();
                log.warn("Transient downstream failure, will redeliver: {}", e.getMessage());
                throw e;
            } catch (PermanentDownstreamException e) {
                metrics.deadLettered("permanent");
                deadLetterRouter.route(message, e.getMessage());
            }
        } finally {
            MDC.remove("jmsMsgId");
        }
    }

    /** Reply correlation: prefer the sender's correlation id, fall back to the message id. */
    private static String correlationId(Message message) throws JMSException {
        String correlationId = message.getJMSCorrelationID();
        return correlationId != null ? correlationId : message.getJMSMessageID();
    }

    private static String safeId(Message message) {
        try {
            return message.getJMSMessageID();
        } catch (JMSException e) {
            return "unknown";
        }
    }
}
