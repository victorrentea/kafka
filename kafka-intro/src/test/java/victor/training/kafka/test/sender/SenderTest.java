package victor.training.kafka.test.sender;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

public class SenderTest extends IntegrationTest {
  @Autowired
  TestedSender testedSender;
  @MockitoBean
  BService bService;

  String message = UUID.randomUUID().toString();

  @Test
  void calls() {
    testedSender.send(message);
    bService.logic(message);
  }
  @Test
  void sends() {
    testedSender.send(message);
    // TODO assert message is sent
    // RISK: a) test consumer competition
    // RISK: b) messages left from previous test
  }
}
