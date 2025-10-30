package victor.training.kafka.test.sender;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.verify;

public class SenderTest extends IntegrationTest {
  @Autowired
  TestedSender testedSender;
  @MockitoBean
  BService bService;

  String message = UUID.randomUUID().toString();

  @Test
  void calls() {
    testedSender.send(message);
    verify(bService).logic(message);
  }

  @Test
  void sends() {
    testedSender.send(message);
    // TODO assert message is sent
    // RISK: a) test consumer competition -> fix: random test group-id, @EmbeddedKafka or 1 testcontainer/test spring context instance
    // RISK: b) messages left from previous test -> fix: drain topics/reset consumer offsets
  }
}
