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

  @RetryableTopic//(attempts = "3", backoff = @Backoff(delay = 1000)) // by spring-kafka
  @KafkaListener(topics = TOPIC, concurrency = "1")
  public void handle(String message) throws InterruptedException {
    log.info("::got \""+message+"\" - pending(=" + pendingOpen + ", pairs()=" + pairs);
    if (message.equals("(")) pendingOpen++;
    if (message.equals(")")) {
      if (pendingOpen == 0) {
        throw new IllegalStateException("Illegal");

  //      Thread.sleep(1000); // RAU: blochezi acest consumer instance = 1 thread; paralizezi toate partiile asociate lui

//        kafkaTemplate.send(TOPIC, message); // RAU: ramai in while(true) daca nu-s alte message

  //      CompletableFuture.runAsync(() -> kafkaTemplate.send(TOPIC, message),
  //          delayedExecutor(1, SECONDS));// RISK: App crash, k8s kill -9, re-deploy
  //          SchedulingTaskExecutor// la kill app termina taskurile

        // @andrei: insert intr-o tabela tot ce vine si apoi cu un cron
        // dau un select pe ce-a venit in ultimele 5min.
        // Bonus de complexitate: mesajele mai vechi de 5 min => erori

        // Rabbit🐰: delayed-send; da' Kafka n-are.
      } else {
        pairs++;
        pendingOpen--;
      }
    }
    log.info("SUCCESS");
  }
}
