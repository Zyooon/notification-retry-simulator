package com.example.notification.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.example.notification.config.RabbitMQConfig;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DlqConsumer {
    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void onDlqMessage(org.springframework.amqp.core.Message message) {
        var props = message.getMessageProperties();
        var headers = props.getHeaders();

        String messageId = props.getMessageId();
        String correlationId = props.getCorrelationId();
        Object xDeath = headers.get("x-death"); // broker가 DLQ로 보냈을 때 유용
        Object origin = headers.get("x-dlq-origin"); // 수동으로 직접 넣는 값
        Object reason = headers.get("x-dlq-reason");
        Object retryCount = headers.get("x-retry-count");

        // payload는 너무 길면 일부만/해시로
        String payload = new String(message.getBody());

        log.error(
            "event=dlq_received msgId={} corrId={} origin={} reason={} retry={} xDeath={} contentType={} payload={}",
            messageId,
            correlationId,
            origin,
            reason,
            retryCount,
            xDeath,
            props.getContentType(),
            payload
        );
    }
}
