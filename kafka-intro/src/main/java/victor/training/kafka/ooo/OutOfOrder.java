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
public class OutOfOrder {
  public static final String TOPIC = "ooo-topic";
  private final KafkaTemplate<String, String> kafkaTemplate;

  public int pendingOpen = 0;
  public int pairs = 0;

  // after 2 attempts 1 sec delayed spring-kafka sends message to xxx-dlt
//  @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000))

  @KafkaListener(topics = TOPIC, concurrency = "1")
  public void handle(String message) throws InterruptedException {
    log.info("⭐️received \"" + message + "\" - pendingOpen=" + pendingOpen + ", pairs()=" + pairs);
    if ("(".equals(message)) pendingOpen++;
    if (")".equals(message)) {
      if (pendingOpen > 0) {
        pairs++;
        pendingOpen--;
      } else { // closing a parenthesis never opened
        throw new IllegalStateException("Illegal"); // ✅ just fail (KISS)

        // ⭐️ ideas to explore:
        // Thread.sleep(1000); // a) ❌ BAD: not a solution + blocks all partitions of this consumer instance

//        kafkaTemplate.send(TOPIC, message); // b) send myself: may immediately re-consume the same message (no wait❌) + needs a counter to prevent infinite loop😱

        //  CompletableFuture.runAsync(() -> kafkaTemplate.send(TOPIC, message),
        //          delayedExecutor(1, SECONDS)); // c) send it to myself "after a while" 😱 RISK: messages lost on app crash / restart

        // @RetryableTopic from spring-kafka // d) ✅

        // e) insert in inbox table and process later on a scheduler - dev effort

        // Rabbit🐰: delayed-send
      }
    }
  }
}
