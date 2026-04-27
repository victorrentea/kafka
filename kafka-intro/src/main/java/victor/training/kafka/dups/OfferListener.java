package victor.training.kafka.dups;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
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

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static java.lang.Thread.sleep;
import static victor.training.kafka.dups.OfferListener.OFFER_TOPIC;

@Slf4j
@RequiredArgsConstructor
class OfferListener {
  static final String OFFER_TOPIC = "offer-topic";
  private final OfferRepo offerRepo;

  // TODO experiment: restart the app while consuming; upon restart it inserts duplicate offers in DB
  // Fix duplicates:
  // 1. change event semantics to "upsert" + add client-generated UUID => Consumer handles a dup create as noop-update
  // 2. adds idempotencyKey to message; consumers saves it in DB (in a separate @Entity); if found in DB => ignore message
  @KafkaListener(topics = OFFER_TOPIC) // ack_mode = batch (default) // =⇒ max 1 duplicate / restart vs throughput🔽
  void consume(OfferCreatedEvent event) throws InterruptedException {
    Offer order = new Offer().name(event.offerName());
    log.info("Saving offer " + order);
    offerRepo.save(order);
    sleep(50);
  }
}

interface OfferRepo extends JpaRepository<Offer, String> {
}

@Data
@Entity
@JsonAutoDetect(fieldVisibility = ANY)
class Offer {
  @Id
  private String id = UUID.randomUUID().toString();
  private String name;
}

record OfferCreatedEvent(String offerName) {
}

@Slf4j
@RequiredArgsConstructor
@RestController
class OfferController {
  private final OfferRepo offerRepo;
  private final KafkaTemplate<String, OfferCreatedEvent> kafkaTemplate;

  @GetMapping(value = "send-offers", produces = "text/html")
    // http://localhost:8080/send-offers
  String sendOffers() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      String offerName = "offer-" + LocalDateTime.now();
      kafkaTemplate.send(OFFER_TOPIC, "k", new OfferCreatedEvent(offerName));
    }
    return "Restart the app within 2-3 seconds, restart it, and <a href='/offers'>check for duplicates in DB</a>";
  }

  @GetMapping("offers")
    // http://localhost:8080/offers
  List<Offer> viewOffersInDB() {
    return offerRepo.findAll();
  }
}

interface PastIdempotencyKeyRepo extends JpaRepository<PastIdempotencyKey, Long> {
}

@Data
@Entity
class PastIdempotencyKey {
  @Id
  @GeneratedValue
  private Long id;
  private String idempotencyKey;
}
