package com.example.notification.api;

import com.example.notification.config.RabbitMQConfig;
import com.example.notification.idempotency.IdempotencyStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationDlqController {

    private final RabbitTemplate rabbitTemplate;
    private final IdempotencyStore idempotencyStore;
    private final StringRedisTemplate redisTemplate;

    private static final String REDIS_DLQ_LIST_KEY = "notifications:dlq:list";

    @PostMapping("/replay")
    public String replayDlqMessages(@RequestParam(defaultValue = "10") int limit) {

        int count = 0;

        var converter = (Jackson2JsonMessageConverter) rabbitTemplate.getMessageConverter();

        while (count < limit) {
            // 1. Redis List에서 가장 오래된(오른쪽) 메시지 꺼내기 (RPOP)
            // DlqConsumer가 왼쪽(LPUSH)으로 넣었으므로, RPOP으로 꺼내면 FIFO 순서가 됨
            String payload = redisTemplate.opsForList().index(REDIS_DLQ_LIST_KEY, -1);
            
            if (payload == null) break;

            try {
// 핵심 수정: payload(String)를 바로 NotificationMessage 클래스로 역직렬화
            // fromMessage 대신 converter 내부의 기능을 활용하거나 직접 변환
            NotificationMessage msg = (NotificationMessage) converter.fromMessage(
                new org.springframework.amqp.core.Message(payload.getBytes()),
                NotificationMessage.class // <--- 여기서 타입을 명시적으로 지정!
            );
                // 3. 멱등성 저장소에서 해당 키 해제 (다시 처리될 수 있도록)
                idempotencyStore.release(msg.idempotencyKey());

                // 4. 메인 큐로 재전송 (재시도 횟수 초기화 및 리플레이 헤더 추가)
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.MAIN_EXCHANGE,
                        RabbitMQConfig.MAIN_KEY,
                        msg.withRetryCount(0), // 재시도 횟수 초기화
                        m -> {
                            m.getMessageProperties().setHeader("x-replay", "true");
                            m.getMessageProperties().setHeader("x-replay-origin", "redis");
                            return m;
                        });

                        // 3. 발송 성공 시에만 Redis에서 실제 삭제 (트랜잭션 활용)
            redisTemplate.execute(new SessionCallback<List<Object>>() {
                @Override
                public List<Object> execute(RedisOperations ops) {
                    ops.multi();
                    // 발송 성공했으니 오른쪽 끝에서 하나를 실제로 제거 (RPOP)
                    ops.opsForList().rightPop(REDIS_DLQ_LIST_KEY); 
                    return ops.exec();
                }
            });
                
                log.info("event=notify_replay_success idemKey={}", msg.idempotencyKey());


                count++;
                
            } catch (Exception e) {
                log.error("event=notify_replay_failed error={}", e.getMessage());
                // 실패한 건 다시 Redis에 넣어두거나 별도 로그 처리를 할 수 있어
            }
        }
        
        return count + " messages replayed from Redis to main queue.";
    }
}