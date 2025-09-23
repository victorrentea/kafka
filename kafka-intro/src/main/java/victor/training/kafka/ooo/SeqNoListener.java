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
public class SeqNoListener {
  public static final String TOPIC = "ooo-seqno-topic";

  public int open = 0;
  public int pairs = 0;

  record SeqMessage(int seqNo, String message){}

  @KafkaListener(topics = TOPIC, concurrency = "1")
  public void handle(SeqMessage message) {
    log.info("::START (=" + open + ", ()=" + pairs + " on " + message);

  }
}
