package victor.training.kafka.dups;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class OrderListenerTest extends IntegrationTest {
  @Autowired
  private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
  @Autowired
  private OrderRepo orderRepo;

  @Test
  void shouldNotInsertDuplicates() throws InterruptedException, ExecutionException {
    orderRepo.deleteAll();
    kafkaTemplate.send(OrderListener.ORDER_TOPIC, new OrderCreatedEvent("M1")).get();
    kafkaTemplate.send(OrderListener.ORDER_TOPIC, new OrderCreatedEvent("M1")).get();

    Thread.sleep(4000);

    assertThat(orderRepo.findAll())
        .hasSize(1)
        .first()
        .returns("M1", Order::contents);
  }
}
