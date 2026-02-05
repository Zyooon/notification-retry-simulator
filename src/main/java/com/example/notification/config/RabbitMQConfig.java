package com.example.notification.config;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.Binding;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class RabbitMQConfig {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQConfig.class);

    @PostConstruct
    void loaded() {
        log.info(">>> RabbitMQConfig LOADED <<<");
    }


    public static final String MAIN_EXCHANGE = "notification.main.exchange";
    public static final String RETRY_EXCHANGE = "notification.retry.exchange";
    public static final String DLQ_EXCHANGE = "notification.dlq.exchange";

    public static final String MAIN_QUEUE = "notification.main.queue";
    public static final String RETRY_QUEUE = "notification.retry.queue";
    public static final String DLQ_QUEUE = "notification.dlq.queue";

    public static final String MAIN_KEY = "notification.main";
    public static final String RETRY_KEY = "notification.retry";
    public static final String DLQ_KEY = "notification.dlq";

    // Exchanges
    @Bean
    DirectExchange mainExchange() {
        return new DirectExchange("notification.main.exchange");
    }

    @Bean
    DirectExchange retryExchange() {
        return new DirectExchange("notification.retry.exchange");
    }

    @Bean
    DirectExchange dlqExchange() {
        return new DirectExchange("notification.dlq.exchange");
    }

    // Queues
    @Bean
    Queue mainQueue() {
        return QueueBuilder.durable("notification.main.queue")
                .deadLetterExchange("notification.retry.exchange")
                .deadLetterRoutingKey("notification.retry")
                .build();
    }

    @Bean
    Queue retryQueue() {
        return QueueBuilder.durable("notification.retry.queue")
                .ttl(5000) // 5초 후 재시도
                .deadLetterExchange("notification.main.exchange")
                .deadLetterRoutingKey("notification.main")
                .build();
    }

    @Bean
    Queue dlqQueue() {
        return QueueBuilder.durable("notification.dlq.queue").build();
    }

    // Bindings
    @Bean
    Binding mainBinding() {
        return BindingBuilder.bind(mainQueue())
                .to(mainExchange())
                .with("notification.main");
    }

    @Bean
    Binding retryBinding() {
        return BindingBuilder.bind(retryQueue())
                .to(retryExchange())
                .with("notification.retry");
    }

    @Bean
    Binding dlqBinding() {
        return BindingBuilder.bind(dlqQueue())
                .to(dlqExchange())
                .with("notification.dlq");
    }
}
