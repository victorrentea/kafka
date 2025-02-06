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
//  public void consume(Event event) throws InterruptedException {
  public void consume(ConsumerRecord<String, Event> record) throws InterruptedException {
    switch (record.value()) {
      case Event.EventOK(String work):
        log.info("Received event: " + work);
        break;
      case Event.EventCausingError e:
        log.error("Received event that causes error");
        throw new RuntimeException("Eroare in procesare");
      default:
        log.error("Unknown record: " + record);
    }
    log.info("Handled " + record);
  }
}
