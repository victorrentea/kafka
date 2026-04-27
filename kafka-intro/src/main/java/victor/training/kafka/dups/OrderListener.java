package victor.training.kafka.dups;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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

@Slf4j
@RequiredArgsConstructor
class OrderListener {
  static final String ORDER_TOPIC = "order-topic";
  private final OrderRepo orderRepo;

  // TODO experiment: restart the app while consuming; upon restart it inserts duplicate orders in DB
  // Fix duplicates:
  // 1. change event semantics to "upsert" + add client-generated UUID => Consumer handles a dup create as noop-update
  // 2. adds idempotencyKey to message; consumers saves it in DB (in a separate @Entity); if found in DB => ignore message
  @KafkaListener(topics = ORDER_TOPIC, batch = "false")
  // ack_mode = batch (default) // =⇒ max 1 duplicate / restart vs throughput🔽
  void consume(OrderCreatedEvent event) throws InterruptedException {
    Order order = new Order().contents(event.orderContents());
    log.info("Saving order " + order);
    orderRepo.save(order);
    sleep(50);
  }
}

interface OrderRepo extends JpaRepository<Order, String> {
}

@Data
@Entity
@Table(name = "orders")
@JsonAutoDetect(fieldVisibility = ANY)
class Order {
  @Id
  private String id = UUID.randomUUID().toString();
  private String contents;
}

record OrderCreatedEvent(String orderContents) {
}

@Slf4j
@RequiredArgsConstructor
@RestController
class OrderController {
  private final OrderRepo orderRepo;
  private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

  @GetMapping(value = "send-orders", produces = "text/html")
    // http://localhost:8080/send-orders
  String sendOrders() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      sleep(1);
      String orderName = "order-" + LocalDateTime.now();
      kafkaTemplate.send(OrderListener.ORDER_TOPIC, "k", new OrderCreatedEvent(orderName));
    }
    return "Restart the app within 2-3 seconds and <a href='/orders'>check for duplicates in DB</a>";
  }

  @GetMapping("orders")
    // http://localhost:8080/orders
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
