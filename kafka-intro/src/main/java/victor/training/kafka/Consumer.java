package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import victor.training.kafka.inbox.Inbox;
import victor.training.kafka.inbox.InboxRepo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {
  private final InboxRepo inboxRepo;

  @KafkaListener(topics = "myTopic")
  public void consume(ConsumerRecord<String, Event> record) throws InterruptedException {
    switch (record.value()) {
      // TODO DELETE:
      case Event.EventCausingError event:
        log.error("Throwing exception for " + event);
        throw new RuntimeException("Exception processing " + event);
      case Event.EventTakingLong event:
        log.error("Long processing {}", event);
        Thread.sleep(12000);
        break;
      case Event.EventOK(String work):
        log.info("Processing work: " + work);
        break;
      case Event.EventForLater event:
        log.info("Inserting in inbox for later processing: " + event);
        long timestampLong = record.timestamp();
        LocalDateTime timestamp =  Instant.ofEpochMilli(timestampLong)
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime();
        try {
          inboxRepo.save(new Inbox(event.work(), timestamp, event.idempotencyKey()));
        } catch (DataIntegrityViolationException ex) {
          log.warn("Ignoring Duplicate event: " + event);
        }
        break;

      default:
        log.error("Unknown record: " + record);
    }
    log.info("Handled of " + record);
    log.info("Handled " + record);
  }
}

