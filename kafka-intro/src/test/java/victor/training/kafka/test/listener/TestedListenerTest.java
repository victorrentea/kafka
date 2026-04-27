package victor.training.kafka.test.listener;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.IntegrationTest;

import java.util.Random;
import java.util.UUID;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static victor.training.kafka.test.listener.TestedListener.IN_TOPIC;

public class TestedListenerTest extends IntegrationTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @MockitoBean
  protected TestedService aServiceMock;

//  @Test
  @RepeatedTest(10) // flaky:
  //  with the app started
  // (2)
  void explore() {
    kafkaTemplate.send(IN_TOPIC, new Random().nextInt(4), UUID.randomUUID().toString(), "m");

    verify(aServiceMock, timeout(3000)).logic("m");
  }
}
