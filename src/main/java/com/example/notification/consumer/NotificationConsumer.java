package com.example.notification.consumer;

import com.example.notification.api.NotificationMessage;
import com.example.notification.config.RabbitMQConfig;
import com.example.notification.idempotency.IdempotencyStore;
import com.example.notification.metrics.ConsumeResult;
import com.example.notification.metrics.NotificationMetrics;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class NotificationConsumer {

    private final RabbitTemplate rabbitTemplate;
    private final IdempotencyStore idempotencyStore;
    private final NotificationMetrics notificationMetrics;
    private final int maxRetryCount;

    public NotificationConsumer(RabbitTemplate rabbitTemplate,
            IdempotencyStore idempotencyStore,
            NotificationMetrics notificationMetrics,
            @Value("${retry.max-count:3}") int maxRetryCount) {
        this.rabbitTemplate = rabbitTemplate;
        this.idempotencyStore = idempotencyStore;
        this.notificationMetrics = notificationMetrics;
        this.maxRetryCount = maxRetryCount;
    }

    @RabbitListener(queues = RabbitMQConfig.MAIN_QUEUE)
    public void onMessage(NotificationMessage msg) {
        long startNs = System.nanoTime();
        ConsumeResult result = null;

        log.info(
                "event=notify_consume_start idemKey={} retryCount={}",
                msg.idempotencyKey(),
                msg.retryCount());

        try {
            // 0) 중복 방지
            boolean acquired = idempotencyStore.tryAcquire(msg.idempotencyKey());
            if (!acquired) {
                result = ConsumeResult.SKIPPED_DUPLICATE;
                log.warn(
                        "event=notify_duplicate_skip idemKey={} retryCount={}",
                        msg.idempotencyKey(),
                        msg.retryCount());
                return;
            }

            // 1) 전송 시도(랜덤 실패)
            boolean fail = ThreadLocalRandom.current().nextInt(100) < 80;

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

        } finally {
            // result가 null이면 로직 버그니까 잡히게 하거나 UNKNOWN 추가해도 됨
            if (result != null) {
                notificationMetrics.recordResult(result);
            }
            notificationMetrics.recordDurationNs(System.nanoTime() - startNs);
        }
    }

}
