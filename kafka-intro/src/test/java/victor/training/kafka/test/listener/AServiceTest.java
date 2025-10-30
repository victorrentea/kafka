package victor.training.kafka.test.listener;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import static org.mockito.Mockito.verify;

public class AServiceTest extends IntegrationTest {
  @MockitoBean ARepo aRepo;
  @Autowired AService aService;

  @Test
  void test() {
    aService.logic("A");
    verify(aRepo).save("A");
  }
}
