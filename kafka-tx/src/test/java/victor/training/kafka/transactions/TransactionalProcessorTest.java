package victor.training.kafka.transactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static victor.training.kafka.transactions.TransactionalProcessor.*;

@TestPropertySource(properties = "spring.kafka.producer.transaction-id-prefix=ktx-")
@Slf4j
@ResetKafkaOffsets({IN_TOPIC, OUT_TOPIC_A, OUT_TOPIC_B})
@SpringBootTest
public class TransactionalProcessorTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  private static final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
  @BeforeEach
  @AfterEach
  final void before() {
      queue.clear();
  }

  @Test
  void ok() throws InterruptedException, ExecutionException {
    kafkaTemplate.executeInTransaction(s ->
        kafkaTemplate.send(TransactionalProcessor.IN_TOPIC, "OK"));

    await()
        .atMost(ofSeconds(5))
        .untilAsserted(() -> assertThat(queue).containsExactlyInAnyOrder("A1", "A2", "B"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"fail-at-step-1","fail-at-step-2","fail-at-step-3","fail-at-step-4"})
  void fail(String message) throws InterruptedException, ExecutionException {
    kafkaTemplate.executeInTransaction(s ->
        kafkaTemplate.send(TransactionalProcessor.IN_TOPIC, message));

    Thread.sleep(5000);

    assertThat(queue).isEmpty();
  }

  @TestConfiguration
  public static class Listener {
    @KafkaListener(topics = {OUT_TOPIC_A, OUT_TOPIC_B},
        groupId = "test",
        properties = {
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG+"=latest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG+"=read_committed"})
    public void listen(String message) {
      log.info("Received: " + message);
      queue.add(message);
    }
  }
}
