package com.muvhulawa.gateway.config;

import jakarta.jms.ConnectionFactory;
import org.springframework.boot.autoconfigure.jms.DefaultJmsListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;

/**
 * The listener container factory used by {@link com.muvhulawa.gateway.inbound.Pacs008Listener}.
 * The decisive setting is {@code sessionTransacted(true)}: message acknowledgement is tied to the
 * outcome of the handler, so a thrown exception rolls the consume back and the broker redelivers.
 * This is the foundation of the gateway's at-least-once, idempotent-downstream delivery guarantee.
 */
@Configuration
public class JmsConfig {

    @Bean
    public DefaultJmsListenerContainerFactory gatewayListenerFactory(
            ConnectionFactory connectionFactory,
            DefaultJmsListenerContainerFactoryConfigurer configurer,
            GatewayProperties properties) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        configurer.configure(factory, connectionFactory);
        factory.setSessionTransacted(true);
        factory.setConcurrency(properties.getConcurrency());
        return factory;
    }
}
