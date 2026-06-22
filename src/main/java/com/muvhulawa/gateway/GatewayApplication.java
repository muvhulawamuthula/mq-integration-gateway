package com.muvhulawa.gateway;

import com.muvhulawa.gateway.config.GatewayProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.jms.annotation.EnableJms;

/**
 * IBM MQ Integration Gateway.
 *
 * <p>Bridges ISO 20022 {@code pacs.008} credit-transfer messages arriving on an MQ queue to the
 * payment processor's HTTP API, and publishes the resulting {@code pacs.002} status report back on
 * a reply queue. The transport is JMS (IBM MQ in production, an embedded Artemis broker in
 * dev/test); the processor stays a plain HTTP service. The gateway is deliberately payload-agnostic
 * — it never parses the XML — so the contract between the two services is just "valid pacs.008 in,
 * pacs.002 out".
 */
@SpringBootApplication
@EnableJms
@EnableConfigurationProperties(GatewayProperties.class)
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
