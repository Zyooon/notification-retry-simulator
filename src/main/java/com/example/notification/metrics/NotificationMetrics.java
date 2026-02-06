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
    private final Timer processing;

    public NotificationMetrics(MeterRegistry registry) {
        this.registry = registry;
        this.publish = Counter.builder("notify_publish_total").register(registry);
        this.success = Counter.builder("notify_success_total").register(registry);
        this.retry = Counter.builder("notify_retry_total").register(registry);
        this.skip = Counter.builder("notify_skip_total").register(registry);
        this.processing = Timer.builder("notify_processing_seconds").register(registry);
    }

    public void recordResult(ConsumeResult result) {
        switch (result) {
            case SUCCESS -> success.increment();
            case RETRY_PUBLISHED -> retry.increment();
            case SKIPPED_DUPLICATE -> skip.increment();
        }
    }

    // DLQ 전용 메서드 추가
    public void recordDlq(String origin, String reason) {
        Counter.builder("notify_dlq_total")
                .description("Total number of messages sent to DLQ")
                .tag("origin", origin)
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    public void recordDurationNs(long durationNs) {
        processing.record(durationNs, TimeUnit.NANOSECONDS);
    }

    public void recordPublish() {
        this.publish.increment();
    }
}
