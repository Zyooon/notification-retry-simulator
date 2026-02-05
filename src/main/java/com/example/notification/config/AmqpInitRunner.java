package com.example.notification.config;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
/**
 * [Bootstrap Runner for Infrastructure Declaration]
 *
 * 이 Runner는 비즈니스 로직을 위한 코드가 아니라,
 * RabbitMQ 인프라(Exchange / Queue / DLQ / TTL)를
 * 애플리케이션 시작 시점에 명시적으로 선언하기 위한 부트스트랩 코드다.
 *
 * 실제 서비스에서는 Consumer(@RabbitListener)나 Producer(RabbitTemplate)가
 * 존재하기 때문에 RabbitMQ 연결 및 인프라 선언이 자연스럽게 트리거된다.
 *
 * 그러나 이 프로젝트는
 * - 메시지 소비/발행 로직보다
 * - 재시도/지연 큐/DLQ 구조 자체를 먼저 검증하고
 * - RabbitMQ Management UI에서 인프라 구성을 시각적으로 확인하기 위한
 *   "운영 시뮬레이션 목적"의 프로젝트이기 때문에
 *
 * AmqpAdmin.initialize()를 애플리케이션 시작 시점에 명시적으로 호출한다.
 *
 * ※ Consumer가 추가되면 본 Runner는 제거하거나 비활성화해도 무방하다.
 */
@Component
public class AmqpInitRunner implements CommandLineRunner {
        private final AmqpAdmin amqpAdmin;

    public AmqpInitRunner(AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
    }

    @Override
    public void run(String... args) {
        amqpAdmin.initialize();
        System.out.println("AMQP ADMIN INITIALIZE DONE");
    }
}
