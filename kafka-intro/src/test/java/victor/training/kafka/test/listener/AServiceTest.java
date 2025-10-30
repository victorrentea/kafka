package victor.training.kafka.test.listener;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import static org.mockito.Mockito.verify;
// to avoid starting multiple spring boot test context escalate,
// any mockbean in the parent test class as a spy Bean
public class AServiceTest extends IntegrationTest {
  @Autowired AService aService;

  @Test
  void test() {
    aService.logic("A");
    verify(aRepo).save("A");
  }
}
