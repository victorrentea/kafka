package victor.training.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.test.listener.ARepo;
import victor.training.kafka.test.listener.AService;

@SpringBootTest
@ActiveProfiles("test")
//@EmbeddedKafka //A) fiecare spring primeste instanta lui de emulator de kafka #nuoface
// or via Kafka from docker-compose.yaml
// B)
// C) SF porneste cate un Kafka testcontainer/instanta de spring in teste https://github.com/PlaytikaOSS/testcontainers-spring-boot
// la modu Purist ori integrezi ori te prefaci cu @MockBean - @silvia
public abstract class IntegrationTest {
  @MockitoSpyBean
  protected ARepo aRepo;
  @MockitoSpyBean
  protected AService aServiceMock;
  @Autowired
  private ApplicationContext applicationContext;

  @BeforeEach
  final void before() {
    String groupId = applicationContext.getEnvironment().getProperty("spring.kafka.consumer.group-id");
    System.out.println("Running with group-id: "+groupId);
  }
}
