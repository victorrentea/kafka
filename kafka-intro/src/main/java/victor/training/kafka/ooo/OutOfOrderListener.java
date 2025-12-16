package victor.training.kafka.ooo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Component
@RequiredArgsConstructor
@RestController
public class OutOfOrderListener {
  public static final String TOPIC = "ooo-topic";
  private final KafkaTemplate<String, String> kafkaTemplate;

  public int pendingOpen = 0;
  public int pairs = 0;

  // @RetryableTopic//(attempts = "3", backoff = @Backoff(delay = 1000)) // then spring-kafka sends message to xxx-dlt
  @KafkaListener(topics = TOPIC, concurrency = "1")
  public void handle(String message) throws InterruptedException {
    log.info("::got \"" + message + "\" - pendingOpen=" + pendingOpen + ", pairs()=" + pairs);
    if ("(".equals(message)) pendingOpen++;
    if (")".equals(message)) {
      if (pendingOpen == 0) {
        throw new IllegalStateException("Illegal");
        // ideas to explore:
        // a) Thread.sleep(1000); // ❌❌ BAD: not a solution + blocks all partitions of this consumer instance
        // b) kafkaTemplate.send(TOPIC, message); // ❌ BAD: may immediately re-consume the same message

        // c) CompletableFuture.runAsync(() -> kafkaTemplate.send(TOPIC, message),
        //          delayedExecutor(1, SECONDS)); // 😱 RISK: messages lost on app crash / restart

        // d) @RetryableTopic from spring-kafka
        // e) insert in inbox table and process later on a scheduler

        // Rabbit🐰: delayed-send
      } else {
        pairs++;
        pendingOpen--;
      }
    }
    log.info("SUCCESS");
  }
}
