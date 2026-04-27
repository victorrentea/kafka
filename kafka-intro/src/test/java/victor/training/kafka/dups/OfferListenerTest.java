package victor.training.kafka.dups;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.dups.OfferCreatedEvent;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class OfferListenerTest extends IntegrationTest {
  @Autowired
  private KafkaTemplate<String, OfferCreatedEvent> kafkaTemplate;
  @Autowired
  private OfferRepo offerRepo;

  @Test
//  @Disabled("FIME")
  void shouldNotInsertDuplicates() throws InterruptedException, ExecutionException {
    kafkaTemplate.send(OfferListener.OFFER_TOPIC, new OfferCreatedEvent("M1")).get();
    kafkaTemplate.send(OfferListener.OFFER_TOPIC, new OfferCreatedEvent("M1")).get();

    Thread.sleep(4000);

    assertThat(offerRepo.findAll())
        .hasSize(1)
        .first()
        .returns("M1", Offer::name);
  }
}
