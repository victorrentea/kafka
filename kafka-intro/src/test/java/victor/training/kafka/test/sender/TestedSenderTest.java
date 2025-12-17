package victor.training.kafka.test.sender;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import victor.training.kafka.testutil.ReceivedKafkaRecord;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class TestedSenderTest extends IntegrationTest {
  @Autowired
  TestedSender testedSender;
  @MockitoBean
  BService bService;
  @Autowired
  ConsumerFactory<String, String> consumerFactory;

  String message = UUID.randomUUID().toString();

  @Test
  void calls() {
    testedSender.send(message);
    verify(bService).logic(message);
  }

  @Test
  void sends(@ReceivedKafkaRecord(TestedSender.OUT_TOPIC)
             CompletableFuture<ConsumerRecord<String, String>> future) throws Exception {
    testedSender.send(message);
    var record = future.get(10, TimeUnit.SECONDS);
    assertThat(record.value()).isEqualTo(message);
  }

  @Test
  void sends_withoutExtension() throws Exception {
    var groupId = "test-" + UUID.randomUUID();
    try (var consumer = consumerFactory.createConsumer(groupId, null, null)) {
      consumer.subscribe(List.of(TestedSender.OUT_TOPIC));
      // asigură asignarea
      consumer.poll(Duration.ofMillis(100));

      testedSender.send(message);

      var record = KafkaTestUtils.getSingleRecord(consumer, TestedSender.OUT_TOPIC, Duration.ofSeconds(10));
      assertThat(record.value()).isEqualTo(message);
    }
  }
}
