package victor.training.kafka.dups;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static victor.training.kafka.dups.OrderListener.ORDER_TOPIC;

public class OrderListenerTest extends IntegrationTest {
  @Autowired
  private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
  @Autowired
  private OrderRepo orderRepo;
  @MockitoSpyBean
  OrderListener orderListener;

  @Test
  void shouldNotInsertDuplicates() throws InterruptedException, ExecutionException {
    orderRepo.deleteAll(); // I like to start on a clean slate.

    UUID ik = UUID.randomUUID(); // V1 -> V4
    kafkaTemplate.send(ORDER_TOPIC,
        new OrderCreatedEvent("M1", ik)).get();
    kafkaTemplate.send(ORDER_TOPIC,
        new OrderCreatedEvent("M1", ik)).get();

    Mockito.verify(orderListener,
        timeout(4000)
            .times(2))
        .consume(any()); // smart, no sleep

    assertThat(orderRepo.findAll())
        .hasSize(1)
        .first()
        .returns("M1", Order::contents);
  }
}
