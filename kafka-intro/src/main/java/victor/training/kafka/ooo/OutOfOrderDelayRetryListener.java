package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Component
@RestController
public class OutOfOrderDelayRetryListener {
  public static final String TOPIC = "ooo-topic";

  public int pendingOpen = 0;
  public int pairs = 0;

  @KafkaListener(topics = TOPIC, concurrency = "1")
  @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 100))
  public void handle(String message) {
    log.info("::got \""+message+"\" - pending=" + pendingOpen + ", pairs=" + pairs);
    if (message.equals("(")) pendingOpen++;
    if (message.equals(")") && pendingOpen == 0)
      throw new IllegalStateException("Illegal");
    pendingOpen--;
    pairs++;
  }
}
