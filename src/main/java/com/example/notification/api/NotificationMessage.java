package com.example.notification.api;

public record NotificationMessage(
        String notificationId,
        int retryCount,
        String idempotencyKey,
        String payload
) {
    public NotificationMessage withRetryCount(int newRetryCount) {
        return new NotificationMessage(
                this.notificationId,
                newRetryCount,
                this.idempotencyKey,
                this.payload
        );
    }
}
    
