package com.muvhulawa.gateway.config;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.springframework.boot.autoconfigure.jms.artemis.ArtemisConfigurationCustomizer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Dev/test only. Gives the embedded Artemis broker the same backout semantics a real IBM MQ queue
 * would have: after a handful of failed (rolled-back) deliveries a message is moved to the
 * dead-letter address instead of redelivering forever. In production (the {@code mq} profile) this
 * is configured on the IBM MQ queue itself via {@code BOTHRESH} (backout threshold) and
 * {@code BOQNAME} (backout requeue name), so no equivalent bean is needed.
 */
@Configuration
@Profile("!mq")
public class EmbeddedBrokerConfig {

    private final GatewayProperties properties;

    public EmbeddedBrokerConfig(GatewayProperties properties) {
        this.properties = properties;
    }

    @org.springframework.context.annotation.Bean
    public ArtemisConfigurationCustomizer backoutPolicyCustomizer() {
        return configuration -> {
            AddressSettings settings = new AddressSettings()
                    .setDeadLetterAddress(SimpleString.toSimpleString(properties.getQueues().getDeadLetter()))
                    .setMaxDeliveryAttempts(3)   // initial try + 2 redeliveries, then dead-letter
                    .setRedeliveryDelay(0);      // no delay — keeps tests fast and dev responsive
            configuration.addAddressSetting("#", settings);
        };
    }
}
