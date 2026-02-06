package com.example.notification.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class NotificationMetrics {

    private static final String METRIC_PUBLISH = "notify_publish_total";
    private static final String METRIC_SUCCESS = "notify_success_total";
    private static final String METRIC_RETRY = "notify_retry_total";
    private static final String METRIC_SKIP = "notify_skip_total";
    private static final String METRIC_REPLAY_SUCCESS = "notify_replay_success_total";
    private static final String METRIC_POISON_PILL = "notify_poison_pill_total";
    private static final String METRIC_DLQ_TOTAL = "notify_dlq_total";
    private static final String METRIC_DEAD_COUNT = "notify_dead_letter_count";
    private static final String METRIC_LATENCY = "notify_processing_seconds";

    private final MeterRegistry registry;
    private final StringRedisTemplate redisTemplate;

    private final Counter publishCounter;
    private final Counter successCounter;
    private final Counter retryCounter;
    private final Counter skipCounter;
    private final Counter replaySuccessCounter;
    private final Counter poisonPillCounter;
    private final Timer processingTimer;

    public NotificationMetrics(MeterRegistry registry, StringRedisTemplate redisTemplate) {
        this.registry = registry;
        this.redisTemplate = redisTemplate;

        this.publishCounter = createCounter(METRIC_PUBLISH, "전체 알림 발행 요청 수");
        this.successCounter = createCounter(METRIC_SUCCESS, "최초 전송 성공 수");
        this.retryCounter = createCounter(METRIC_RETRY, "재시도 큐 발행 수");
        this.skipCounter = createCounter(METRIC_SKIP, "멱등성에 의한 중복 제거 수");
        this.replaySuccessCounter = createCounter(METRIC_REPLAY_SUCCESS, "DLQ에서 메인 큐로 복구 성공한 수");
        this.poisonPillCounter = createCounter(METRIC_POISON_PILL, "데이터 결함으로 인해 격리된 메시지 수");
        
        this.processingTimer = Timer.builder(METRIC_LATENCY).register(registry);

        registerDeadLetterGauge();
    }

    // 카운터 생성을 위한 헬퍼 메서드
    private Counter createCounter(String name, String description) {
        return Counter.builder(name).description(description).register(registry);
    }

    // 게이지 등록 메서드 분리
    private void registerDeadLetterGauge() {
        Gauge.builder(METRIC_DEAD_COUNT, () -> {
            Long size = redisTemplate.opsForList().size("notifications:dlq:dead");
            return size != null ? size.doubleValue() : 0.0;
        })
        .description("현재 격리소(Dead)에 쌓인 메시지 개수")
        .register(registry);
    }

    // --- 기록 메서드들 ---

    public void recordResult(ConsumeResult result) {
        switch (result) {
            case SUCCESS -> successCounter.increment();
            case RETRY_PUBLISHED -> retryCounter.increment();
            case SKIPPED_DUPLICATE -> skipCounter.increment();
        }
    }

    public void recordDlq(String origin, String reason) {
        // 태그를 동적으로 추가해야 하는 지표는 여기서 직접 빌드
        Counter.builder(METRIC_DLQ_TOTAL)
                .tag("origin", origin)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    public void recordReplaySuccess() { replaySuccessCounter.increment(); }

    public void recordDurationNs(long durationNs) {
        processingTimer.record(durationNs, TimeUnit.NANOSECONDS);
    }

    public void recordPublish() { publishCounter.increment(); }

    public void recordPoisonPill() { poisonPillCounter.increment(); }
}