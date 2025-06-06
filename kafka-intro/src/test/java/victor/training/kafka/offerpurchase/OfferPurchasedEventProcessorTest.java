package victor.training.kafka.offerpurchase;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.sim.SimEvent;
import victor.training.kafka.sim.SimEvent.CreditAdded;
import victor.training.kafka.sim.SimEvent.OfferActivated;
import victor.training.kafka.sim.SimEventListener;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@TestPropertySource(properties = "spring.kafka.producer.transaction-id-prefix=ktx-")
public class OfferPurchasedEventProcessorTest extends KafkaTest {
  @Autowired
  private KafkaTemplate<String, OfferPurchasedEvent> kafkaTemplate;
  private static final BlockingQueue<SimEvent> queue = new LinkedBlockingQueue<>();

  @Test
  void transactedTransformation() throws InterruptedException, ExecutionException {
    var inputEvent = new OfferPurchasedEvent(1L, "Off", 100);
    kafkaTemplate.executeInTransaction(s ->
      kafkaTemplate.send(OfferPurchasedEventProcessor.OFFER_PURCHASED_TOPIC, inputEvent)
    );
    var m1 = queue.poll(2, TimeUnit.SECONDS);
    var m2 = queue.poll(2, TimeUnit.SECONDS);

    assertThat(Stream.of(m1,m2)).containsExactly(
        new CreditAdded(1L, 100),
        new OfferActivated(1L, "Off", 100));
  }

  @TestConfiguration
  public static class Listener {
    @KafkaListener(topics = SimEventListener.SIM_TOPIC,
        groupId = "TestListener",
        properties = "auto.offset.reset=latest"
    )
    public void listen(SimEvent event) {
      System.out.println("GOT EVENT: "+ event);
      queue.add(event);
    }
  }
}
