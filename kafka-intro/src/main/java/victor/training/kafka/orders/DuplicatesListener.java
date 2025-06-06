package victor.training.kafka.orders;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class DuplicatesListener {
  private final OrderRepo orderRepo;

  @KafkaListener(topics = "duplicates")
  public void consume(ConsumerRecord<?, String> record) {
    log.info("Process {}", record);
    String data = record.value();
    var order = new Order().data(data);
    orderRepo.save(order);
    randomlyFail();
  }

  private void randomlyFail() {
    if (Math.random() < .7) {
      log.error("Boom!");
      throw new IllegalArgumentException("Boom");
    }
    log.info("Create other entities");
  }

}
