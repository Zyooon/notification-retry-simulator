package com.example.notification.api;

import com.example.notification.config.RabbitMQConfig;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/notifications")
public class NotificationPublishController {

    private final RabbitTemplate rabbitTemplate;

    public NotificationPublishController(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    @PostMapping
    public PublishResponse publish(@RequestBody PublishRequest req) {
        String notificationId = UUID.randomUUID().toString();
        String idempotencyKey = req.idempotencyKey() != null ? req.idempotencyKey() : notificationId;

        NotificationMessage msg = new NotificationMessage(
                notificationId,
                0,
                idempotencyKey,
                req.payload()
        );

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.MAIN_EXCHANGE,
                RabbitMQConfig.MAIN_KEY,
                msg
        );

        return new PublishResponse(notificationId, idempotencyKey);
    }

    public record PublishRequest(String payload, String idempotencyKey) {}
    public record PublishResponse(String notificationId, String idempotencyKey) {}
}
