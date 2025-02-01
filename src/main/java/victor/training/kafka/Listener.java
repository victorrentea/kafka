package victor.training.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class Listener {
  @SneakyThrows
  @KafkaListener(topics = "myTopic")
  public void listen(Event message) {
    log.info("Start: " + message);
    if (message instanceof Event1 e1 && e1.name().equals("FAIL")) {
      throw new RuntimeException("Simulated exception");
    }
    Thread.sleep(100);
    log.info("Done:  " + message);
  }


}
