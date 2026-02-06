package com.example.notification.service;


import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.notification.api.NotificationMessage;
import com.example.notification.config.RabbitMQConfig;
import com.example.notification.metrics.NotificationMetrics;

import lombok.RequiredArgsConstructor;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class NotificationPublishService {

    private final RabbitTemplate rabbitTemplate;
    private final NotificationMetrics metrics;
    
    public NotificationResult sendNotification(String sequenceId, String payload) {

        String idempotencyKey = UUID.randomUUID().toString();

        NotificationMessage msg = new NotificationMessage(
                sequenceId,
                0,
                idempotencyKey,
                payload
        );

        try {
            // 발행 지표 기록
            metrics.recordPublish();

            // RabbitMQ 전송
            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.MAIN_EXCHANGE,
                    RabbitMQConfig.MAIN_KEY,
                    msg
            );
            
            // 성공 결과 반환
            return new NotificationResult(sequenceId, idempotencyKey, true);
            
        } catch (Exception e) {
            // 실패 시 결과 반환 (필요시 예외를 던져도 돼)
            return new NotificationResult(sequenceId, idempotencyKey, false);
        }
    }
    public record NotificationResult(String id, String key, boolean isSuccess) {}
}

