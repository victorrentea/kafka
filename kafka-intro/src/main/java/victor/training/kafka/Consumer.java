package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
    var event = record.value();
    switch (event) {
      case Event.EventCausingError e:
        log.error("Throwing exception for " + e);
        throw new RuntimeException("Exception processing " + event);
      case Event.EventTakingLong e:
        log.error("Long processing {}", e);
        Thread.sleep(12000);
        break;
      case Event.EventOK(String work):
        log.info("Processing work: " + work);
        break;
      case Event.EventForLater e:
        log.info("Processing later: " + e);
        long timestampLong = record.timestamp();
        LocalDateTime timestamp =  Instant.ofEpochMilli(timestampLong)
            .atZone(ZoneId.systemDefault())
            .toLocalDateTime();
        inboxRepo.save(new Inbox(e.work(), timestamp));
        break;
      default:
        log.error("Unknown event: " + event);
    }
    log.info("Normal completion of " + event);
  }
}
