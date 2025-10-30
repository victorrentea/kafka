package victor.training.kafka.offer;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
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
  // - idempotent semantics: "upsert" => "nothing to do"
  // - message.idempotency key found in DB => "I've seen this message before"

  public record OfferCreatedEvent(String offerName) {
  }

  @KafkaListener(topics = OFFER_TOPIC)
  public void consume(OfferCreatedEvent event) throws InterruptedException {
    var order = new Offer().name(event.offerName());
    offerRepo.save(order);
    Thread.sleep(1000);
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

interface OfferRepo extends JpaRepository<Offer, Long> {
}

@Data
@Entity
class Offer {
  @Id
  @GeneratedValue
  private Long id;
  private String name;
}
