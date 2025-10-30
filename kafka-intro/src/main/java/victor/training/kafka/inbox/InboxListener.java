package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@RestController
@RequiredArgsConstructor
public class InboxListener {
  public static final String TOPIC = "inbox-topic";
  private final InboxRepo inboxRepo;
  private final KafkaTemplate<String, Message> kafkaTemplate;

  @GetMapping("send-inbox") // http://localhost:8080/send-inbox
  public void send() {
    for (int i = 0; i < 20; i++) {
      kafkaTemplate.send(TOPIC, new Message("data"+i, "idem"+i));
    }
  }

  record Message(String data, String idempotencyKey) {}

  @KafkaListener(topics = TOPIC)
  public void consume(ConsumerRecord<String, Message> record) throws InterruptedException {
    Message message = record.value();
    log.info("Inserting in inbox: " + message);
    LocalDateTime timestamp = timestampToLocalDateTime(record.timestamp());
    try {
      Inbox inbox = new Inbox(message.data(), timestamp, message.idempotencyKey())
          .priority(determinePriority(message));
      inboxRepo.save(inbox);
    } catch (DataIntegrityViolationException ex) {
      log.warn("Ignoring Duplicate event: " + record.value() + " due to " + ex);
    }
  }

  private Integer determinePriority(Message message) {
    if (message.data.startsWith("+")) return 1; // higher priority
    return 10;
  }

  private LocalDateTime timestampToLocalDateTime(long timestampLong) {
    return Instant.ofEpochMilli(timestampLong)
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime();
  }
}

