package victor.training.kafka.ooo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RestController;

import static java.util.concurrent.CompletableFuture.delayedExecutor;

@Slf4j
@Component
@RequiredArgsConstructor
@RestController
public class OutOfOrderListener {
  public static final String TOPIC = "ooo-topic";
  private final KafkaTemplate<String, String> kafkaTemplate;

  public int pendingOpen = 0;
  public int pairs = 0;

  // @RetryableTopic//(attempts = "3", backoff = @Backoff(delay = 1000)) // then send to xxx-dlt spring-kafka
  @KafkaListener(topics = TOPIC, concurrency = "1")
  public void handle(String message) throws InterruptedException {
    log.info("::got \""+message+"\" - pendingOpen=" + pendingOpen + ", pairs()=" + pairs);
    if ("(".equals(message)) pendingOpen++;
    if (")".equals(message)) {
      if (pendingOpen == 0) {
        throw new IllegalStateException("Illegal");
        // ideas to explore:

  //      Thread.sleep(1000); // BAD: blocks all partitions of this consumer instance

//        kafkaTemplate.send(TOPIC, message); // BAD: immediate re-consumer the same  message immediately if no other

  //      CompletableFuture.runAsync(() -> kafkaTemplate.send(TOPIC, message),
  //          delayedExecutor(1, SECONDS));// RISK: App crash / k8s kill -9 / re-deploy

        // insert in inbox table and process later on a scheduler

        // Rabbit🐰: delayed-send
      } else {
        pairs++;
        pendingOpen--;
      }
    }
    log.info("SUCCESS");
  }
}
