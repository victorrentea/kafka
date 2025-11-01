package victor.training.kafka.test.listener;

import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static victor.training.kafka.test.listener.TestedListener.IN_TOPIC;

public class TestedListenerTest extends IntegrationTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @RepeatedTest(10)
//  @Test
  void explore() {
    kafkaTemplate.send(IN_TOPIC, UUID.randomUUID().toString(), "m");

    verify(aServiceMock, timeout(20000)).logic("m");
  }
}
