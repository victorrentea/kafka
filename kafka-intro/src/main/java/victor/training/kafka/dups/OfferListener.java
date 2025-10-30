package victor.training.kafka.dups;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static java.lang.Thread.sleep;

@Slf4j
@RequiredArgsConstructor
@RestController
public class OfferListener {
  public static final String OFFER_TOPIC = "offer-topic";
  private final OfferRepo offerRepo;

  // Fix duplicates:
  // 0. ackMode = record instead of batch = partial solution reduces dup risk to 1 message max/partition
  // 1. client-generated [external] ID => raise error: "already created"
  // 2. change event semantics to "upsert" => ignore: "nothing to do"
  // 3. add message.idempotency key; save it in DB (new @Entity); if found in DB => ignore: "nothing to do"

  public record OfferCreatedEvent(UUID id, String offerName) {
//  public record OfferCreatedEvent(String offerName) {
  }

  @KafkaListener(topics = OFFER_TOPIC) // ack_mode = batch (default)
  public void consume(OfferCreatedEvent event) throws InterruptedException {
    Offer order = new Offer().id(event.id().toString()).name(event.offerName());
    offerRepo.save(order); // INSERT or UPDATE cu aceleasi date = NOOP
    sleep(50);
  }

  private final KafkaTemplate<String, OfferCreatedEvent> kafkaTemplate;
  @GetMapping("send-offers") // http://localhost:8080/send-offers
  public String sendOffers() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      kafkaTemplate.send(OFFER_TOPIC, "k", new OfferCreatedEvent(UUID.randomUUID(), "offer-" + System.currentTimeMillis()));
    }
    return "Kill the app in 2-3 seconds, restart it, and check for duplicates in DB";
  }
}

interface OfferRepo extends JpaRepository<Offer, Long> {
}

@Data
@Entity
class Offer {
  @Id
//  @GeneratedValue
  private String id;
  private String name;
}
