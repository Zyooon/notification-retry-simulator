package com.example.notification.api;

import com.example.notification.service.NotificationDlqService;

import lombok.RequiredArgsConstructor;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationDlqController {

    private final NotificationDlqService dlqService;

    @PostMapping("/replay")
    public String replayDlqMessages(@RequestParam(defaultValue = "10") int limit) {
        int replayedCount = dlqService.replayMessages(limit);
        return String.format("%d messages replayed from Redis to main queue.", replayedCount);
    }
}