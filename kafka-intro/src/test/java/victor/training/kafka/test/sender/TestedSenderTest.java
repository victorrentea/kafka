package victor.training.kafka.test.sender;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

import static org.mockito.Mockito.verify;

public class TestedSenderTest extends IntegrationTest {
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
    // TODO assert message is sent - WIPWIP 🚧🚧🚧🚧
    // RISK: a) test consumer competition with multiple Spring apps -> fix: random test group-id, @EmbeddedKafka or 1 testcontainer/test spring context instance
    // RISK: b) messages left from previous test -> fix: drain topics/reset consumer offsets
  }
}
