package victor.training.kafka.dups;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static java.lang.Thread.sleep;

@Slf4j
@Component
@RequiredArgsConstructor
class OrderListener {
  static final String ORDER_TOPIC = "order-topic";
  private final OrderRepo orderRepo;

  // TODO experiment: restart the app while consuming; upon restart it inserts duplicate orders in DB
  // ☢️ Duplicates:
  // Fix#1: change event semantics to "upsert" + add client-generated UUID => Consumer handles a dup create as noop-update
  // Fix#2: adds idempotencyKey to message; consumers saves it in DB (see PastIdempotencyKey @Entity); if found in DB => ignore message
//  @Transactional
  @KafkaListener(topics = ORDER_TOPIC)
  // in yaml: ack_mode = batch (default) // =⇒ max 1 duplicate / restart vs throughput🔽
  void consume(OrderCreatedEvent event) throws InterruptedException {
    Order order = new Order()
        .contents(event.orderContents())
        .id(event.futureOrderUUID()); // client-generated IDs ⇒ creation idempotency
    log.info("Saving order " + order);

    // Hash the order content and check in the database that there is no other order with the same hash already.
    // if (repo.containsOrderWithHash(hash(event.orderContents)) return; // NOOP

    try {
      orderRepo.save(order);
    } catch (DataIntegrityViolationException e) {
      log.info("Duplicated ignored");
    }
    sleep(50);
    log.info("Saved order");
  }
}

interface OrderRepo extends JpaRepository<Order, String> {
}

@Data
@Entity
@Accessors(fluent = true, chain = true)
@Table(name = "orders")
@JsonAutoDetect(fieldVisibility = ANY)
class Order {
  @Id
  private String id;
  private String contents;
}

record OrderCreatedEvent(String orderContents, String futureOrderUUID) {
}

// --- support code for manual testing ---

@Slf4j
@RequiredArgsConstructor
@RestController
class OrderController {
  private final OrderRepo orderRepo;
  private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

  // http://localhost:8080/send-orders
  @GetMapping(value = "send-orders", produces = "text/html")
  String sendOrders() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      String orderName = "order-" + LocalDateTime.now();
      kafkaTemplate.send(OrderListener.ORDER_TOPIC, "k", new OrderCreatedEvent(orderName, UUID.randomUUID().toString()));
    }
    return "Restart the app within 2-3 seconds and <a href='/orders'>check for duplicates in DB</a>";
  }

  // http://localhost:8080/orders
  @GetMapping("orders")
  List<Order> viewOrdersInDB() {
    return orderRepo.findAll();
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
