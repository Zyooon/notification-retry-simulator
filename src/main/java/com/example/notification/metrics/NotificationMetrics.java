package com.example.notification.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class NotificationMetrics {

    private final MeterRegistry registry;
    private final Counter publish;
    private final Counter success;
    private final Counter retry;
    private final Counter skip;
    private final Counter replaySuccess;
    private final Counter poisonPill;
    private final Timer processing;

    public NotificationMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.publish = Counter.builder("notify_publish_total")
                .description("전체 알림 발행 요청 수")
                .register(registry);
        this.success = Counter.builder("notify_success_total")
                .description("최초 전송 성공 수")
                .register(registry);
        this.retry = Counter.builder("notify_retry_total")
                .description("재시도 큐 발행 수")
                .register(registry);
        this.skip = Counter.builder("notify_skip_total")
                .description("멱등성에 의한 중복 제거 수")
                .register(registry);
        this.replaySuccess = Counter.builder("notify_replay_success_total")
                .description("DLQ에서 메인 큐로 복구 성공한 수")
                .register(registry);
        this.poisonPill = Counter.builder("notify_poison_pill_total")
            .description("데이터 결함으로 인해 격리된 메시지 수")
            .register(registry);       
        this.processing = Timer.builder("notify_processing_seconds")
                .register(registry);
    }

    public void recordResult(ConsumeResult result) {
        switch (result) {
            case SUCCESS -> success.increment();
            case RETRY_PUBLISHED -> retry.increment();
            case SKIPPED_DUPLICATE -> skip.increment();
        }
    }

    // DLQ 전용: 어떤 단계(origin)에서 어떤 이유(reason)로 실패했는지 기록
    public void recordDlq(String origin, String reason) {
        Counter.builder("notify_dlq_total")
                .description("Total number of messages sent to DLQ")
                .tag("origin", origin)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    // 추가: Replay 성공 시 호출
    public void recordReplaySuccess() {
        this.replaySuccess.increment();
    }

    public void recordDurationNs(long durationNs) {
        processing.record(durationNs, TimeUnit.NANOSECONDS);
    }

    public void recordPublish() {
        this.publish.increment();
    }
    public void recordPoisonPill() {
    this.poisonPill.increment();
}
}