package victor.training.kafka.duplicates;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class DuplicatesListener {
  @KafkaListener(topics = "duplicates")
  public void consume(String message) {
    log.info("Process " + message);
    if (Math.random() < .5) {
      System.out.println("Boom!");
      throw new IllegalArgumentException("Boom");
    }
  }
}
