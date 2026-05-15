package victor.training.kafka.dups;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.UUID;

// Manual demo for at-least-once redelivery of uncommitted batches.
//
// Run flow:
//   1. docker-compose up -d
//   2. mvn spring-boot:run -pl kafka-intro     <- standalone app, starts consuming
//   3. Run this test                            <- floods 300 distinct orders
//   4. Ctrl+C the app mid-consumption
//   5. mvn spring-boot:run -pl kafka-intro again
//   6. http://localhost:8080/orders            <- duplicates from the redelivered uncommitted batch
//
// NOT @EmbeddedKafka: messages must survive the test JVM exit.
// listener auto-startup disabled so this JVM does not consume its own messages.
@SpringBootTest(properties = "spring.kafka.listener.auto-startup=false")
@ActiveProfiles("test")
public class FloodOrdersTest {
  @Autowired
  private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

  @Test
  void send300DistinctOrders() {
    // random 3 letters
    String rand = String.valueOf((char) ('a' + (int) (Math.random() * 26))) +
        (char) ('a' + (int) (Math.random() * 26)) +
        (char) ('a' + (int) (Math.random() * 26));
    for (int i = 0; i < 100; i++) {
      kafkaTemplate.send(OrderListener.ORDER_TOPIC, "k", new OrderCreatedEvent("order-"+rand+"-" + i,
          UUID.randomUUID().toString()));
    }
  }
}
