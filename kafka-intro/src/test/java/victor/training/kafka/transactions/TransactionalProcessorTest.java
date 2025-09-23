package victor.training.kafka.transactions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import victor.training.kafka.KafkaTest;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.transactions.TransactionalProcessor.*;

@TestPropertySource(properties = "spring.kafka.producer.transaction-id-prefix=ktx-")
@Slf4j
public class TransactionalProcessorTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  private static final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

  @Test
  void ok() throws InterruptedException, ExecutionException {
    kafkaTemplate.executeInTransaction(s ->
        kafkaTemplate.send(TransactionalProcessor.IN, "OK"));

    List<String> sentMessages = Arrays.asList(
        queue.poll(2, SECONDS),
        queue.poll(1, SECONDS),
        queue.poll(1, SECONDS));

    assertThat(sentMessages).containsExactlyInAnyOrder("A1","A2","B");
  }

  @ParameterizedTest
  @ValueSource(strings = {"1","2","3","4"})
  void fail(String message) throws InterruptedException, ExecutionException {
    kafkaTemplate.executeInTransaction(s ->
        kafkaTemplate.send(TransactionalProcessor.IN, message));

    List<String> sentMessages = Arrays.asList(
        queue.poll(2, SECONDS),
        queue.poll(1, SECONDS),
        queue.poll(1, SECONDS));

    assertThat(sentMessages).containsOnlyNulls();
  }

  @TestConfiguration
  public static class Listener {
    @KafkaListener(topics = {OUT_A, OUT_B},
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
