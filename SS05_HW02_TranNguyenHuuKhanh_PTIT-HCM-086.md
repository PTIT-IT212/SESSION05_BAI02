# BÀI 2: Tối ưu Prompt cho Notification Service bất đồng bộ

## 1. Nội dung Prompt sau khi tối ưu

```text
Hãy đóng vai trò là một Solution Architect kiêm Senior Java Spring Boot Developer.

Bối cảnh:
Tôi đang xây dựng Notification Service cho hệ thống thương mại điện tử. Khi có sự kiện khuyến mãi, hệ thống cần gửi email và SMS cho khoảng 1 triệu người dùng. Nếu gửi đồng bộ trực tiếp trong request của người dùng thì API có nguy cơ timeout, nghẽn thread và làm giảm trải nghiệm người dùng.

Nhiệm vụ:
Hãy tư vấn kiến trúc xử lý bất đồng bộ phù hợp trước khi viết code. Không được mặc định chọn ngay @Async.

Yêu cầu phân tích:
1. Đề xuất ít nhất 3 phương án kiến trúc để gửi thông báo bất đồng bộ trong Spring Boot, ví dụ:
   - Spring @Async + ThreadPoolTaskExecutor
   - Message Queue như RabbitMQ hoặc Kafka
   - Job Scheduler / Batch Worker
2. Lập bảng so sánh trade-offs giữa các phương án theo các tiêu chí:
   - Khả năng chịu tải
   - Độ phức tạp triển khai
   - Độ tin cậy khi gửi thất bại
   - Khả năng retry
   - Khả năng scale horizontal
   - Chi phí vận hành
3. Phân tích các kịch bản giả định:
   - Nếu có 1 triệu user và gửi trong vòng 5 phút thì hệ thống cần xử lý bao nhiêu message/giây?
   - Nếu email provider bị rate limit thì cần xử lý thế nào?
   - Nếu một worker bị crash giữa chừng thì làm sao tránh mất message?
   - Nếu hệ thống cần mở rộng từ 1 server lên nhiều server thì phương án nào phù hợp nhất?
4. Sau khi phân tích, hãy chọn một phương án khuyến nghị cho production.
5. Cuối cùng, cung cấp mã nguồn Java Spring Boot minh họa cho phương án được chọn, gồm:
   - API nhận yêu cầu gửi campaign
   - Producer đẩy message vào queue
   - Consumer xử lý gửi email/SMS
   - Retry cơ bản khi gửi thất bại
   - Logging bằng @Slf4j

Ràng buộc:
- Sử dụng Java 17 và Spring Boot 3.
- Ưu tiên thiết kế dễ mở rộng, có khả năng retry và không làm timeout API chính.
- Trình bày bằng tiếng Việt.
```

## 2. Bảng so sánh giải pháp kiến trúc do AI sinh ra

```markdown
| Phương án | Ưu điểm | Nhược điểm | Phù hợp khi |
|---|---|---|---|
| Spring @Async + ThreadPoolTaskExecutor | Dễ triển khai, ít phụ thuộc hạ tầng, phù hợp hệ thống nhỏ | Không bền vững nếu server crash, khó retry mạnh, khó scale cho 1 triệu message | Số lượng thông báo thấp hoặc hệ thống nội bộ nhỏ |
| RabbitMQ | Hỗ trợ queue bền vững, ack/nack, retry, dead-letter queue, dễ tách producer-consumer | Cần vận hành message broker, throughput thấp hơn Kafka ở quy mô cực lớn | Gửi email/SMS cần độ tin cậy và xử lý từng message rõ ràng |
| Kafka | Throughput rất cao, phù hợp streaming và hàng triệu message, scale tốt | Phức tạp hơn RabbitMQ, cần quản lý offset, ordering và consumer group | Hệ thống lớn, có nhiều loại event, cần xử lý dữ liệu luồng |
| Batch Scheduler | Dễ kiểm soát tốc độ gửi, phù hợp gửi theo chiến dịch | Không real-time, cần thiết kế bảng trạng thái và job retry | Campaign marketing theo lịch, không cần gửi ngay lập tức |
```

## 3. Phân tích giả định tải cao

Nếu cần gửi cho 1,000,000 người dùng trong 5 phút:

```text
5 phút = 300 giây
Throughput yêu cầu = 1,000,000 / 300 = khoảng 3,334 message/giây
```

Với tải này, `@Async` trên một ứng dụng đơn lẻ là không phù hợp vì thread pool có giới hạn và không đảm bảo message không bị mất khi server crash. Phương án hợp lý hơn là dùng message queue để tách API chính khỏi tiến trình gửi notification.

Khuyến nghị production: dùng **RabbitMQ** nếu ưu tiên độ tin cậy, retry, dead-letter queue và xử lý từng notification rõ ràng. Nếu hệ thống đã có Kafka hoặc cần throughput cực lớn, có thể chọn Kafka.

## 4. Mã nguồn Java minh họa với RabbitMQ

```java
package com.example.notification;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/campaigns")
@RequiredArgsConstructor
class CampaignController {

    private final NotificationProducer notificationProducer;

    @PostMapping("/send")
    public ResponseEntity<String> sendCampaign(@RequestBody CampaignRequest request) {
        notificationProducer.publishCampaign(request);
        return ResponseEntity.accepted().body("Campaign accepted for async processing");
    }
}

@Service
@RequiredArgsConstructor
@Slf4j
class NotificationProducer {

    private final RabbitTemplate rabbitTemplate;

    public void publishCampaign(CampaignRequest request) {
        for (Long userId : request.userIds()) {
            NotificationMessage message = new NotificationMessage(
                    userId,
                    request.title(),
                    request.content(),
                    request.channels()
            );
            rabbitTemplate.convertAndSend(RabbitMQConfig.NOTIFICATION_EXCHANGE,
                    RabbitMQConfig.NOTIFICATION_ROUTING_KEY,
                    message);
        }
        log.info("Published notification campaign, totalUsers={}", request.userIds().size());
    }
}

@Component
@RequiredArgsConstructor
@Slf4j
class NotificationConsumer {

    private final NotificationSender notificationSender;

    @RabbitListener(queues = RabbitMQConfig.NOTIFICATION_QUEUE)
    public void consume(NotificationMessage message) {
        log.info("Consuming notification for userId={}", message.userId());
        notificationSender.sendWithRetry(message);
    }
}

@Service
@Slf4j
class NotificationSender {

    public void sendWithRetry(NotificationMessage message) {
        int maxAttempts = 3;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                send(message);
                log.info("Notification sent successfully, userId={}", message.userId());
                return;
            } catch (Exception ex) {
                log.warn("Send notification failed, userId={}, attempt={}", message.userId(), attempt, ex);
                if (attempt == maxAttempts) {
                    throw ex;
                }
            }
        }
    }

    private void send(NotificationMessage message) {
        if (message.channels().contains("EMAIL")) {
            log.info("Sending email to userId={}", message.userId());
        }
        if (message.channels().contains("SMS")) {
            log.info("Sending SMS to userId={}", message.userId());
        }
    }
}

@Configuration
class RabbitMQConfig {
    public static final String NOTIFICATION_EXCHANGE = "notification.exchange";
    public static final String NOTIFICATION_QUEUE = "notification.queue";
    public static final String NOTIFICATION_ROUTING_KEY = "notification.send";

    @Bean
    org.springframework.amqp.core.TopicExchange notificationExchange() {
        return new org.springframework.amqp.core.TopicExchange(NOTIFICATION_EXCHANGE);
    }

    @Bean
    org.springframework.amqp.core.Queue notificationQueue() {
        return org.springframework.amqp.core.QueueBuilder.durable(NOTIFICATION_QUEUE).build();
    }

    @Bean
    org.springframework.amqp.core.Binding notificationBinding() {
        return org.springframework.amqp.core.BindingBuilder
                .bind(notificationQueue())
                .to(notificationExchange())
                .with(NOTIFICATION_ROUTING_KEY);
    }
}

record CampaignRequest(String title, String content, List<Long> userIds, List<String> channels) {
}

record NotificationMessage(Long userId, String title, String content, List<String> channels) {
}
```
