package victor.training.kafka.test.listener;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static victor.training.kafka.test.listener.TestedListener.IN_TOPIC;

public class TestRaceListenerTest extends IntegrationTest {
  @MockitoBean
  AService aService;
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

//  @RepeatedTest(10)
  @Test
  void explore() {
    kafkaTemplate.send(IN_TOPIC, UUID.randomUUID().toString(), "m");

    verify(aService, timeout(4000)).logic("m");
  }
}
