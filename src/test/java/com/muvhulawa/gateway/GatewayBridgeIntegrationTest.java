package com.muvhulawa.gateway;

import com.sun.net.httpserver.HttpServer;
import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Drives the gateway end to end against an embedded Artemis broker and a stubbed processor (a tiny
 * in-JVM HTTP server), covering the four behaviours that matter for an MQ bridge:
 *
 * <ol>
 *   <li>a valid message is forwarded and its pacs.002 reply published, correlated to the request;</li>
 *   <li>a transient downstream failure is retried and finally dead-lettered by the broker;</li>
 *   <li>a permanent (4xx) failure is dead-lettered immediately, with no pointless retries;</li>
 *   <li>a non-text payload is treated as poison and dead-lettered.</li>
 * </ol>
 */
@SpringBootTest
@ActiveProfiles("dev")
class GatewayBridgeIntegrationTest {

    // --- stub processor (JDK HTTP server, no extra dependencies) --------------------------------

    record Stub(int code, String paymentStatus, String body) { }

    private static final String PACS002_ACSC = """
            <?xml version="1.0" encoding="UTF-8"?>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.002.001.10">
              <FIToFIPmtStsRpt><GrpHdr><MsgId>STS-1</MsgId></GrpHdr></FIToFIPmtStsRpt>
            </Document>""";

    private static volatile Function<String, Stub> behavior =
            req -> new Stub(200, "ACSC", PACS002_ACSC);

    private static final HttpServer STUB = startStub();

    private static HttpServer startStub() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/v1/payments/pacs008", exchange -> {
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Stub stub = behavior.apply(body);
                byte[] out = stub.body().getBytes(StandardCharsets.UTF_8);
                if (stub.paymentStatus() != null && !stub.paymentStatus().isEmpty()) {
                    exchange.getResponseHeaders().add("X-Payment-Status", stub.paymentStatus());
                }
                exchange.getResponseHeaders().add("X-Idempotent-Replay", "false");
                exchange.sendResponseHeaders(stub.code(), out.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(out);
                }
            });
            server.start();
            return server;
        } catch (IOException e) {
            throw new IllegalStateException("could not start stub processor", e);
        }
    }

    @DynamicPropertySource
    static void processorUrl(DynamicPropertyRegistry registry) {
        registry.add("gateway.processor.base-url",
                () -> "http://127.0.0.1:" + STUB.getAddress().getPort());
    }

    @AfterAll
    static void stopStub() {
        STUB.stop(0);
    }

    // --- fixtures -------------------------------------------------------------------------------

    private static final String VALID_PACS008 = """
            <?xml version="1.0" encoding="UTF-8"?>
            <Document xmlns="urn:iso:std:iso:20022:tech:xsd:pacs.008.001.08">
              <FIToFICstmrCdtTrf><GrpHdr><MsgId>MSG-1</MsgId></GrpHdr></FIToFICstmrCdtTrf>
            </Document>""";

    @Autowired private JmsTemplate jms;

    @BeforeEach
    void reset() {
        behavior = req -> new Stub(200, "ACSC", PACS002_ACSC);
        drain("PACS002.OUT");
        drain("DLQ");
    }

    @Test
    void validMessageIsForwardedAndReplyPublishedWithCorrelation() throws Exception {
        jms.convertAndSend("PACS008.IN", VALID_PACS008, m -> {
            m.setJMSCorrelationID("CORR-1");
            return m;
        });

        Message reply = receive("PACS002.OUT", 8000);
        assertNotNull(reply, "a pacs.002 reply should be published");
        assertEquals("CORR-1", reply.getJMSCorrelationID());
        assertEquals("ACSC", reply.getStringProperty("payment_status"));
        assertInstanceOf(TextMessage.class, reply);
        assertTrue(((TextMessage) reply).getText().contains("FIToFIPmtStsRpt"));

        assertNull(receive("DLQ", 300), "nothing should be dead-lettered on the happy path");
    }

    @Test
    void transientFailureIsRetriedThenDeadLetteredByTheBroker() throws Exception {
        behavior = req -> new Stub(503, "", "service unavailable");

        jms.convertAndSend("PACS008.IN", VALID_PACS008);

        // After the broker's delivery attempts are exhausted the original message lands on the DLQ...
        Message dead = receive("DLQ", 10000);
        assertNotNull(dead, "an exhausted transient message must be dead-lettered");
        assertInstanceOf(TextMessage.class, dead);
        assertTrue(((TextMessage) dead).getText().contains("MSG-1"));
        // ...and no business reply was ever produced.
        assertNull(receive("PACS002.OUT", 300));
    }

    @Test
    void permanentFailureIsDeadLetteredImmediately() throws Exception {
        behavior = req -> new Stub(400, "", "bad request");

        jms.convertAndSend("PACS008.IN", VALID_PACS008);

        Message dead = receive("DLQ", 8000);
        assertNotNull(dead, "a 4xx must be routed straight to the DLQ");
        String reason = dead.getStringProperty("gateway_dlq_reason");
        assertNotNull(reason, "the gateway should tag why it dead-lettered the message");
        assertTrue(reason.contains("400") || reason.toLowerCase().contains("rejected"), reason);
        assertNull(receive("PACS002.OUT", 300));
    }

    @Test
    void nonTextPayloadIsTreatedAsPoison() throws Exception {
        jms.send("PACS008.IN", session -> {
            BytesMessage bytes = session.createBytesMessage();
            bytes.writeUTF("not a pacs.008");
            return bytes;
        });

        Message dead = receive("DLQ", 8000);
        assertNotNull(dead);
        String reason = dead.getStringProperty("gateway_dlq_reason");
        assertNotNull(reason);
        assertTrue(reason.toLowerCase().contains("non-text"), reason);
    }

    // --- helpers --------------------------------------------------------------------------------

    private Message receive(String queue, long timeoutMs) {
        jms.setReceiveTimeout(timeoutMs);
        return jms.receive(queue);
    }

    private void drain(String queue) {
        jms.setReceiveTimeout(150);
        while (jms.receive(queue) != null) {
            // discard
        }
    }
}
