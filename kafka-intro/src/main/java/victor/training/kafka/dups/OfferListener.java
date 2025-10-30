package victor.training.kafka.dups;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static java.lang.Thread.sleep;

@Slf4j
@RequiredArgsConstructor
@RestController
public class OfferListener {
  public static final String OFFER_TOPIC = "offer-topic";
  private final OfferRepo offerRepo;

  // Fix duplicates:
  // 1. client-generated [external] ID => raise error: "already created"
  // 2. change event semantics to "upsert" => ignore: "nothing to do"
  // 3. add message.idempotency key; save it in DB (new @Entity); if found in DB => ignore: "nothing to do"

  public record OfferCreatedEvent(String offerName) {
  }

  @KafkaListener(topics = OFFER_TOPIC)
  public void consume(OfferCreatedEvent event) throws InterruptedException {
    var order = new Offer().name(event.offerName());
    offerRepo.save(order);
    sleep(50);
//    randomlyFail();
  }

  private void randomlyFail() {
    if (Math.random() < .7) {
      log.error("Boom!");
      throw new IllegalArgumentException("Boom");
    }
    log.info("Create other entities");
  }

  private final KafkaTemplate<String, OfferCreatedEvent> kafkaTemplate;
  @GetMapping("send-offers") // http://localhost:8080/send-offers
  public String sendOffers() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      kafkaTemplate.send(OFFER_TOPIC, new OfferCreatedEvent("offer-" + System.currentTimeMillis()));
    }
    return "Restart the app and check for duplicates in DB";
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
