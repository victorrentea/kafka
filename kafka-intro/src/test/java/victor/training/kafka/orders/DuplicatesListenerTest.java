package victor.training.kafka.orders;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.orders.DuplicatesListener.OrderCreatedEvent;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
//@EmbeddedKafka // or via Kafka from docker-compose.yaml
public class DuplicatesListenerTest {

  @Autowired
  private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
  @Autowired
  private OrderRepo orderRepo;

  @Test
  void explore() throws InterruptedException {
    var id= UUID.randomUUID().toString(); // "Universally-Unique..."
    kafkaTemplate.send("duplicates", new OrderCreatedEvent(id,"M1"));
    var newUUID= UUID.randomUUID().toString();
    kafkaTemplate.send("duplicates", new OrderCreatedEvent(newUUID,"M1"));
    Thread.sleep(4000);// how much?
    assertThat(orderRepo.findAll())
        .hasSize(1)
        .first()
        .extracting(Order::data)
        .isEqualTo("M1");
  }
}
