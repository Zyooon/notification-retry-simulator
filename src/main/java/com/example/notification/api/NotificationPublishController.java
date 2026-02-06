package com.example.notification.api;

import lombok.RequiredArgsConstructor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.example.notification.service.NotificationPublishService;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationPublishController {

    private final NotificationPublishService service;

    @PostMapping
    public ResponseEntity<PublishResponse> publish(@RequestBody PublishRequest req) {
        // 서비스 호출 및 결과 수신
        NotificationPublishService.NotificationResult result = service.sendNotification(req.id(), req.payload());

        if (result.isSuccess()) {
            return ResponseEntity.ok(new PublishResponse(
                    result.id(),
                    result.key(),
                    "Success"));
        } else {
            // 전송 실패 시 500 에러 처리
            return ResponseEntity.internalServerError().build();
        }
    }

    public record PublishRequest(String payload, String id) {
    }

    public record PublishResponse(String notificationId, String idempotencyKey, String status) {
    }
}
