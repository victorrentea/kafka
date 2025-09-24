package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Component
@RequiredArgsConstructor
public class InboxListener {
  public static final String TOPIC = "inbox-topic";
  private final InboxRepo inboxRepo;

  record Message(String data, String idempotencyKey) {}

  @KafkaListener(topics = TOPIC)
  public void consume(ConsumerRecord<String, Message> record) throws InterruptedException {
    Message message = record.value();
    log.info("Inserting in inbox for later processing: " + message);
    LocalDateTime timestamp = timestampToLocalDateTime(record.timestamp());
    try {
      inboxRepo.save(new Inbox(message.data(), timestamp, message.idempotencyKey()))
          .priority(determinePriority(message));
    } catch (DataIntegrityViolationException ex) {
      log.warn("Ignoring Duplicate event: " + record.value());
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

