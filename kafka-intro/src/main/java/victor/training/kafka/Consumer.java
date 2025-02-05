package victor.training.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import victor.training.kafka.Event.Event1;

@Component
@Slf4j
public class Consumer {

  @KafkaListener(topics = "myTopic")
  public void consume(Event event) throws InterruptedException {
    if (event instanceof Event1(String name) && "FAIL".equals(name)) {
      log.error("Throwing exception");
      throw new RuntimeException("Exception processing " + event);
    }
    if (event instanceof Event1(String name) && "SLOW".equals(name)) {
      log.error("Long processing ...");
      Thread.sleep(12000);
      log.info("Long processing DONE");
    }
    Thread.sleep(100);
    log.info("Done:  " + event);
  }
}
