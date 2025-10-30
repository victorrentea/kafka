package victor.training.kafka.dups;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class OfferListenerTest extends KafkaTest {
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private OfferRepo offerRepo;

  @Test
  @Disabled("TODO")
  void shouldNotInsertDuplicates() throws InterruptedException, ExecutionException {
    kafkaTemplate.send(OfferListener.OFFER_TOPIC, "M1")
        .get(); // wait for Kafka Producer IO thread to get the Ack from Broker

    Thread.sleep(4000);

    assertThat(offerRepo.findAll())
        .hasSize(1)
        .first()
        .returns("M1", Offer::name);
  }
}
