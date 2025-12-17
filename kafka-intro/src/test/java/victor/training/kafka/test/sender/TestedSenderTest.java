package victor.training.kafka.test.sender;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import scala.sys.Prop;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.core.env.Environment;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.core.ConsumerFactory;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class TestedSenderTest extends IntegrationTest {
  @Autowired
  TestedSender testedSender;
  @MockitoBean
  BService bService;
  @Autowired
  Environment env;
  @Autowired
  ConsumerFactory<String, String> consumerFactory;

  String message = UUID.randomUUID().toString();

  @Test
  void calls() {
    testedSender.send(message);
    verify(bService).logic(message);
  }

  @Test
  void sends() {
    Properties props = new Properties();
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    String consumerGroupId = "test" /*+ "-" + UUID.randomUUID()*/;
    try (Consumer<String, String> consumer = consumerFactory.createConsumer(consumerGroupId, null, null, props)) {
      consumer.subscribe(List.of(TestedSender.OUT_TOPIC));
      // Ensure assignment before sending
      long start = System.currentTimeMillis();
      while (consumer.assignment().isEmpty() && System.currentTimeMillis() - start < 3000) {
        consumer.poll(Duration.ofMillis(100));
      }

      testedSender.send(message);

      ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, TestedSender.OUT_TOPIC, Duration.ofSeconds(10));
      assertThat(record.value()).isEqualTo(message);
    }
  }
}
