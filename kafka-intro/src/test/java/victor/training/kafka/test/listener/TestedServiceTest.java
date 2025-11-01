package victor.training.kafka.test.listener;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import victor.training.kafka.IntegrationTest;

import static org.mockito.Mockito.verify;
// to avoid starting multiple spring boot test context,
// escalate any MockBean in the parent test class as a SpyBean
public class TestedServiceTest extends IntegrationTest {
  @Autowired
  TestedService aService;

  @Test
  void test() {
    aService.logic("A");
    verify(aRepo).save("A");
  }
}
