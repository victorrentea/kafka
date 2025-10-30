package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static victor.training.kafka.ooo.OutOfOrderListener.TOPIC;

@Slf4j
@ResetKafkaOffsets({TOPIC, TOPIC + "-retry", TOPIC + "-dlt"})
public class OutOfOrderListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  OutOfOrderListener outOfOrderListener;

  @BeforeEach
  final void before() {
    outOfOrderListener.pairs = 0;
    outOfOrderListener.pendingOpen = 0;
  }

  @Test
  void ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrderListener.pairs).isEqualTo(1));
  }

  @Test
  void ok2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrderListener.pairs).isEqualTo(2));
  }

  @Test
  void scrambled() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrderListener.pairs).isEqualTo(1));
  }

  @Test
  void scrambled2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrderListener.pairs).isEqualTo(2));
  }

}
