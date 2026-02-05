package com.example.notification.metrics;

public enum ConsumeResult {
    SUCCESS,
    RETRY_PUBLISHED,
    SENT_TO_DLQ,
    SKIPPED_DUPLICATE
}