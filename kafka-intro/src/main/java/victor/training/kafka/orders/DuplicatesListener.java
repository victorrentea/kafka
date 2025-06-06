package victor.training.kafka.orders;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class DuplicatesListener {
  private final OrderRepo orderRepo;

  public record OrderCreatedEvent(String id, String data) {}

  @KafkaListener(topics = "duplicates")
  public void consume(ConsumerRecord<?, OrderCreatedEvent> record) {
    log.info("Process {}", record);
    String data = record.value().data();
    var order = new Order()
        .id(record.value().id) // PK/UK of the created object <- Idempotency-Key of the message
        .data(data);
    orderRepo.save(order);
    randomlyFail();
  }

  private void randomlyFail() {
    if (Math.random() < .7) {
      log.error("Boom!");
      throw new IllegalArgumentException("Boom");
    }
    log.info("Create other entities / send out another message");
  }

}
