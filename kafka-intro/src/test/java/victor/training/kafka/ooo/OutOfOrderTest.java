package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static victor.training.kafka.ooo.OutOfOrder.TOPIC;

@Slf4j
//@DrainKafkaTopics({TOPIC, TOPIC + "-retry", TOPIC + "-dlt"})
public class OutOfOrderTest extends IntegrationTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  OutOfOrder outOfOrder;

  @BeforeEach
  final void before() {
    outOfOrder.pairs = 0;
    outOfOrder.pendingOpen = 0;
  }

  @Test
  void one_set() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(5)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(1));
  }

  @Test
  void two_sets() {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(2));
  }


  @Test
  void one_set_scrambled() {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(1));
  }

  @Test
  void two_sets_scrambled2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(2));
  }

  @Test
  void one_closed_may_trigger_infinite_loop_if_resending() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");

    Thread.sleep(1000); // just look in the log
  }

}
