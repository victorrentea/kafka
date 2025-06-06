package victor.training.kafka.orders;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
//@EmbeddedKafka // or via Kafka from docker-compose.yaml
public class DuplicatesListenerTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private OrderRepo orderRepo;

  @Test
  void explore() throws InterruptedException {
    kafkaTemplate.send("duplicates", "M1");
    Thread.sleep(4000);
    assertThat(orderRepo.findAll())
        .hasSize(1)
        .first()
        .extracting(Order::data)
        .isEqualTo("M1");
  }
}
