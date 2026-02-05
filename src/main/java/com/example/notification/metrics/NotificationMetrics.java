package com.example.notification.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class NotificationMetrics {

    private final Counter success;
    private final Counter retry;
    private final Counter dlq;
    private final Counter skip;
    private final Timer processing;

    public NotificationMetrics(MeterRegistry meterRegistry) {
        this.success = Counter.builder("notify_success_total").register(meterRegistry);
        this.retry = Counter.builder("notify_retry_total").register(meterRegistry);
        this.dlq = Counter.builder("notify_dlq_total").register(meterRegistry);
        this.skip = Counter.builder("notify_skip_total").register(meterRegistry);
        this.processing = Timer.builder("notify_processing_seconds").register(meterRegistry);
    }

    public void recordResult(ConsumeResult result) {
        switch (result) {
            case SUCCESS -> success.increment();
            case RETRY_PUBLISHED -> retry.increment();
            case SENT_TO_DLQ -> dlq.increment();
            case SKIPPED_DUPLICATE -> skip.increment();
        }
    }

    public void recordDurationNs(long durationNs) {
        processing.record(durationNs, TimeUnit.NANOSECONDS);
    }
}
