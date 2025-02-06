package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import victor.training.kafka.inbox.Inbox;
import victor.training.kafka.inbox.InboxRepo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {
  private final InboxRepo inboxRepo;

  @KafkaListener(topics = "myTopic")
//  public void consume(Event event) throws InterruptedException {
  @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 1000))
  // => #1 DLT unde sunt trimise mesajele care nu pot fi procesate
  // => #2 retry-topic unde sunt trimise mesajele pentru retry 1 data
  public void consume(ConsumerRecord<String, Event> record) throws InterruptedException {
    switch (record.value()) {
      case Event.EventOK(String work):
        log.info("Received event: " + work);
        break;
      case Event.EventCausingError e:
        log.error("Received event that causes error");
        throw new RuntimeException("Eroare in procesare "+ record.partition() + ":" +record.offset());
      case Event.EventForLater(String work, UUID ik):
        LocalDateTime timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        try {
          inboxRepo.save(new Inbox(work, timestamp, ik));
        } catch (DataIntegrityViolationException e) {
          log.warn("Duplicate event: " + work);
        }
        break;
      default:
        log.error("Unknown record: " + record);
    }
    log.info("Handled " + record);
  }
}
