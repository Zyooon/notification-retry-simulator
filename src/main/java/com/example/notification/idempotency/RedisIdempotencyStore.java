package com.example.notification.idempotency;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RedisIdempotencyStore implements IdempotencyStore {

    private final StringRedisTemplate redis;
    private final Duration processingTtl;
    private final Duration doneTtl;

    public RedisIdempotencyStore(
            StringRedisTemplate redis,
            @Value("${idempotency.processing-ttl-sec:60}") long processingTtlSec,
            @Value("${idempotency.done-ttl-sec:86400}") long doneTtlSec
    ) {
        this.redis = redis;
        this.processingTtl = Duration.ofSeconds(processingTtlSec);
        this.doneTtl = Duration.ofSeconds(doneTtlSec);
    }

    private String k(String key) {
        return "idemp:" + key;
    }

    @Override
    public boolean tryAcquire(String key) {
        // SETNX: 없을 때만 set
        // value는 상태를 남겨 디버깅/운영 확인에 도움
        Boolean ok = redis.opsForValue().setIfAbsent(k(key), "PROCESSING", processingTtl);
        return Boolean.TRUE.equals(ok);
    }

    @Override
    public void markDone(String key) {
        // DONE으로 바꾸고 TTL 길게
        redis.opsForValue().set(k(key), "DONE", doneTtl);
    }

    @Override
    public void release(String key) {
        // 실패하면 락 해제 → 재시도에서 다시 처리권 획득 가능
        redis.delete(k(key));
    }
}
