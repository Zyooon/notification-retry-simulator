package com.example.notification.consumer;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.example.notification.config.RabbitMQConfig;
import com.example.notification.metrics.NotificationMetrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumer {

    private final NotificationMetrics notificationMetrics;
    private final StringRedisTemplate redisTemplate;

    private static final String REDIS_DLQ_LIST_KEY = "notifications:dlq:list";

    @RabbitListener(queues = RabbitMQConfig.DLQ_QUEUE)
    public void onDlqMessage(org.springframework.amqp.core.Message message) {
        var props = message.getMessageProperties();
        var headers = props.getHeaders();

        // 헤더 정보 추출
        String origin = String.valueOf(headers.getOrDefault("x-dlq-origin", "automatic"));
        String reason = String.valueOf(headers.getOrDefault("x-dlq-reason", "unknown"));
        String payload = new String(message.getBody());

        // Prometheus 메트릭 기록
        notificationMetrics.recordDlq(origin, reason);

        // Redis List에 push
        redisTemplate.opsForList().leftPush(REDIS_DLQ_LIST_KEY, payload);

        // 로그 기록
        log.error("event=dlq_processed origin={} reason={} msgId={} payload_saved=true",
                origin, reason, props.getMessageId());
    }
}
