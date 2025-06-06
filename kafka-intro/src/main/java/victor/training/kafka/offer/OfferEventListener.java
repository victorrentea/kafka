package victor.training.kafka.offer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class OfferEventListener {
  public static final String OFFER_TOPIC = "offer-topic";
  private final OfferRepo offerRepo;

  // Fix duplicates:
  // - client-generated [external] ID => "already created"
  // - idempotent semantics => "nothing to do"
  // - idempotency key in DB => "I've seen this message before"


  @KafkaListener(topics = OFFER_TOPIC)
  public void consume(OfferCreatedEvent event) {
    var order = new Offer().name(event.name());
    offerRepo.save(order);
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
