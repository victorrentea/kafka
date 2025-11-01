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

import java.util.List;
import java.util.UUID;

import static java.lang.Thread.sleep;

@Slf4j
@RequiredArgsConstructor
@RestController
public class OfferListener {
  public static final String OFFER_TOPIC = "offer-topic";
  private final OfferRepo offerRepo;
  private final KafkaTemplate<String, OfferCreatedEvent> kafkaTemplate;

  // TODO experiment: kill app while consuming; upon restart it inserts duplicates in DB
  // Fix duplicates:
  // 1. change ackMode to 'record' instead of 'batch' (default) => max 1 dup message/partition; vs throughput loss
  // 2. change event semantics to "upsert" + add client-generated UUID => Consumer handles dup create as 'update'
  // 3. add message.idempotencyKey; save it in DB (in a new @Entity); if found in DB => ignore

  public record OfferCreatedEvent(String offerName) {
  }

  @KafkaListener(topics = OFFER_TOPIC) // ack_mode = batch (default)
  public void consume(OfferCreatedEvent event) throws InterruptedException {
    Offer order = new Offer().name(event.offerName());
    offerRepo.save(order);
    sleep(50);
  }
  @GetMapping("send-offers") // http://localhost:8080/send-offers
  public String sendOffers() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      String offerName = "offer-" + System.currentTimeMillis();
      kafkaTemplate.send(OFFER_TOPIC, "k", new OfferCreatedEvent(offerName));
    }
    return "Kill the app in 2-3 seconds, restart it, and check for duplicates in DB";
  }
  @GetMapping("offers") // http://localhost:8080/offers
  public List<Offer> viewOffers() {
    return offerRepo.findAll();
  }
}

interface OfferRepo extends JpaRepository<Offer, Long> {
}

@Data
@Entity
class Offer {
  @Id
  @GeneratedValue
  private String id;
  private String name;
}
