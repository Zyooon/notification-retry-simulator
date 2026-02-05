package com.example.notification.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "idempotency")
public record IdempotencyProperties(
        long processingTtlSec,
        long doneTtlSec
) {}
