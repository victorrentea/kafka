package victor.training.kafka;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.test.listener.ARepo;
import victor.training.kafka.test.listener.TestedService;
import victor.training.kafka.testutil.ConsumerLagAsserter;

@SpringBootTest
@ActiveProfiles("test")
//@EmbeddedKafka //A) fiecare spring primeste instanta lui de emulator de kafka #nuoface
// or via Kafka from docker-compose.yaml
// B)
// C) SF porneste cate un Kafka testcontainer/instanta de spring in teste https://github.com/PlaytikaOSS/testcontainers-spring-boot
// la modu Purist ori integrezi ori te prefaci cu @MockBean - @silvia
public abstract class IntegrationTest {
  protected final Logger log = LoggerFactory.getLogger(IntegrationTest.class);
  @MockitoSpyBean
  protected ARepo aRepo;
  @MockitoSpyBean
  protected TestedService aServiceMock;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired
  private ConsumerLagAsserter consumerLagAsserter;

  @BeforeEach
  final void before() {
    String groupId = applicationContext.getEnvironment().getProperty("spring.kafka.consumer.group-id");
    log.info("Running spring {} with group-id: {}", applicationContext, groupId);
    waitForPartitionsToBeAssigned();
    consumerLagAsserter.assertNoUnconsumedMessagesInListenerTopics();
  }

  @AfterEach
  final void verifyNoMessageIsRemainsUnconsumed() {
    consumerLagAsserter.assertNoUnconsumedMessagesInListenerTopics();
  }

  @Autowired
  private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

  @SneakyThrows
  private void waitForPartitionsToBeAssigned() {
    // TODO there should be a smarter way ...
    log.info("Waiting for partitions assignment...");
    while (true) {
      var started= kafkaListenerEndpointRegistry.getAllListenerContainers().stream()
          .flatMap(container -> container.getAssignedPartitions().stream())
          .anyMatch(tp-> tp.topic().equals("myTopic") && tp.partition() == 0);

      if (started) {
         log.info("Resuming Test");
        return;
      }
      Thread.sleep(50);
    }
  }
}
