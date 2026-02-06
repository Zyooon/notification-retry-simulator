package com.example.notification.consumer;

import com.example.notification.api.NotificationMessage;
import com.example.notification.config.RabbitMQConfig;
import com.example.notification.idempotency.IdempotencyStore;
import com.example.notification.metrics.ConsumeResult;
import com.example.notification.metrics.NotificationMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.micrometer.common.lang.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final RabbitTemplate rabbitTemplate;
    private final IdempotencyStore idempotencyStore;
    private final NotificationMetrics notificationMetrics;
    private final StringRedisTemplate redisTemplate;
    private final NotificationMetrics metrics;
    private final ObjectMapper objectMapper;

    private final int maxRetryCount = 3;

    @RabbitListener(queues = RabbitMQConfig.MAIN_QUEUE)
    public void onMessage(@NonNull NotificationMessage msg) {
        long startNs = System.nanoTime();
        ConsumeResult result = null;

        log.info(
                "consume : event=notify_consume_start idemKey={} retryCount={}",
                msg.idempotencyKey(),
                msg.retryCount());

        try {

            // --- 장애 유입(Fault Injection) 테스트 구간 ---
            if (msg.payload().contains("FORCE_NULL")) {
                throw new NullPointerException("의도적인 Null 발생");
            }
            if (msg.payload().contains("FORCE_ARG")) {
                throw new IllegalArgumentException("부적절한 인자값");
            }
            // -------------------------------------------

            // 중복 방지
            boolean acquired = idempotencyStore.tryAcquire(msg.idempotencyKey());
            if (!acquired) {
                result = ConsumeResult.SKIPPED_DUPLICATE;
                log.warn(
                        "event=notify_duplicate_skip idemKey={} retryCount={}",
                        msg.idempotencyKey(),
                        msg.retryCount());
                return;
            }

            // 전송 시도(랜덤 실패)
            boolean fail = ThreadLocalRandom.current().nextInt(100) < 80; // 실패율 80퍼

            if (!fail) {
                // 성공 처리
                idempotencyStore.markDone(msg.idempotencyKey());
                result = ConsumeResult.SUCCESS;
                log.info(
                        "event=notify_success idemKey={} retryCount={}",
                        msg.idempotencyKey(),
                        msg.retryCount());
                return;
            }

            // 실패 시: 처리권 해제(재시도 허용)
            idempotencyStore.release(msg.idempotencyKey());

            int nextRetry = msg.retryCount() + 1;

            if (nextRetry > maxRetryCount) {
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.DLQ_EXCHANGE,
                        RabbitMQConfig.DLQ_KEY,
                        msg.withRetryCount(nextRetry), // record에 withRetryCount 만들어두면 편함
                        message -> {
                            message.getMessageProperties().setHeader("x-dlq-origin", "manual");
                            message.getMessageProperties().setHeader("x-dlq-reason", "retry_exceeded");
                            message.getMessageProperties().setHeader("x-retry-count", msg.retryCount());
                            return message;
                        });
                result = ConsumeResult.SENT_TO_DLQ;
                return;
            }

            log.warn(
                    "event=notify_retry_published idemKey={} retryCount={} nextRetry={}",
                    msg.idempotencyKey(),
                    msg.retryCount(),
                    nextRetry);

            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.RETRY_EXCHANGE,
                    RabbitMQConfig.RETRY_KEY,
                    msg.withRetryCount(nextRetry),
                    message -> {
                        message.getMessageProperties().setHeader("x-origin", "retry");
                        message.getMessageProperties().setHeader("x-retry-count", nextRetry);
                        return message;
                    });
            ;
            result = ConsumeResult.RETRY_PUBLISHED;

        } catch (NullPointerException | IllegalArgumentException e) {
            // 데이터 결함형 에러
            log.error("데이터 결함으로 인한 재처리 불가: {}. 메시지 격리 처리.", e.getMessage());

            // 지표 기록
            metrics.recordPoisonPill();

            redisTemplate.opsForList().leftPush("notifications:dlq:dead", msg.toString());

            return;

        } catch (Exception e) {
            log.error("처리 중 에러 발생: {}", e.getMessage());

            // 일반적인 실패는 기존의 handleFailure 로직 수행
            handleFailure(msg);
            result = ConsumeResult.RETRY_PUBLISHED;

        } finally {
            // result가 null이면 로직 버그니까 잡히게 하거나 UNKNOWN 추가해도 됨
            if (result != null) {
                notificationMetrics.recordResult(result);
            }
            notificationMetrics.recordDurationNs(System.nanoTime() - startNs);
        }
    }

    // NotificationConsumer.java 내부

    private void handleFailure(NotificationMessage msg) {
        int nextRetry = msg.retryCount() + 1;

        if (nextRetry > maxRetryCount) {
            // 최대 재시도 횟수 초과 시 DLQ로 전송
            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.DLQ_EXCHANGE,
                    RabbitMQConfig.DLQ_KEY,
                    msg.withRetryCount(nextRetry),
                    message -> {
                        message.getMessageProperties().setHeader("x-dlq-origin", "manual");
                        message.getMessageProperties().setHeader("x-dlq-reason", "retry_exceeded");
                        return message;
                    });
            log.error("event=notify_dlq_sent idemKey={} retryCount={}", msg.idempotencyKey(), nextRetry);
        } else {
            // 재시도 횟수가 남았으면 재시도 큐로 전송
            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.RETRY_EXCHANGE,
                    RabbitMQConfig.RETRY_KEY,
                    msg.withRetryCount(nextRetry),
                    message -> {
                        message.getMessageProperties().setHeader("x-origin", "retry");
                        message.getMessageProperties().setHeader("x-retry-count", nextRetry);
                        return message;
                    });
            log.warn("event=notify_retry_published idemKey={} nextRetry={}", msg.idempotencyKey(), nextRetry);
        }
    }

}
