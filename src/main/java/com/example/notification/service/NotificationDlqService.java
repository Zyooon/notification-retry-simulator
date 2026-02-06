package com.example.notification.service;

import com.example.notification.api.NotificationMessage;
import com.example.notification.config.RabbitMQConfig;
import com.example.notification.idempotency.IdempotencyStore;
import com.example.notification.metrics.NotificationMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDlqService {

    private final RabbitTemplate rabbitTemplate;
    private final IdempotencyStore idempotencyStore;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final NotificationMetrics metrics;

    private static final String REDIS_DLQ_LIST_KEY = "notifications:dlq:list";

    @PostMapping("/replay")
    public int replayMessages(int limit) {

        int count = 0;

        while (count < limit) {
            // 1. Redis List에서 가장 오래된(오른쪽) 메시지 꺼내기 (RPOP)
            // DlqConsumer가 왼쪽(LPUSH)으로 넣었으므로, RPOP으로 꺼내면 FIFO 순서가 됨
            String payload = redisTemplate.opsForList().index(REDIS_DLQ_LIST_KEY, -1);

            if (payload == null)
                break;

            try {

                // 핵심 수정: payload(String)를 바로 NotificationMessage 클래스로 역직렬화
                // fromMessage 대신 converter 내부의 기능을 활용하거나 직접 변환
                NotificationMessage msg = objectMapper.readValue(payload, NotificationMessage.class);

                // 멱등성 저장소에서 해당 키 해제 (다시 처리될 수 있도록)
                idempotencyStore.release(msg.idempotencyKey());

                metrics.recordReplaySuccess();

                // 메인 큐로 재전송 (재시도 횟수 초기화 및 리플레이 헤더 추가)
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.MAIN_EXCHANGE,
                        RabbitMQConfig.MAIN_KEY,
                        msg.withRetryCount(0), // 재시도 횟수 초기화
                        m -> {
                            m.getMessageProperties().setHeader("x-replay", "true");
                            m.getMessageProperties().setHeader("x-replay-origin", "redis");
                            return m;
                        });

                // 발송 성공 시에만 Redis에서 실제 삭제 (트랜잭션 활용)
                redisTemplate.opsForList().rightPop(REDIS_DLQ_LIST_KEY);

                log.info("event=notify_replay_success idemKey={}", msg.idempotencyKey());

                count++;

            } catch (JsonProcessingException | NullPointerException | IllegalArgumentException e) {
                // 데이터 결함형 에러
                log.error("데이터 결함으로 인한 재처리 불가: {}. 메시지 격리 처리.", e.getMessage());

                // 무한 루프 방지를 위해 일단 큐에서 제거하고 별도 보관
                redisTemplate.opsForList().rightPop(REDIS_DLQ_LIST_KEY);
                redisTemplate.opsForList().leftPush("notifications:dlq:dead", payload);

                continue;

            } catch (Exception e) {
                // 데이터는 정상이지만 환경 문제로 실패한 경우 > 루프 중단
                log.error("시스템 일시 장애로 인한 재처리 중단: {}. 데이터는 Redis에 유지.", e.getMessage());
                break;
            }
        }

        return count;
    }
}