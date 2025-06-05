package victor.training.kafka.duplicates;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
//@EmbeddedKafka
public class DuplicatesTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Test
  void explore() throws InterruptedException {
      kafkaTemplate.send("duplicates", "M1");
      Thread.sleep(4000);
  }
}
