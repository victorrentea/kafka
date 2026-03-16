package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.testutil.DrainKafkaTopics;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static victor.training.kafka.ooo.OutOfOrder.TOPIC;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
@DrainKafkaTopics({TOPIC, TOPIC + "-retry", TOPIC + "-dlt"})
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
  void t1_one_set_ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(5)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(1));
  }

  @Test
  void t2_two_sets_ok() {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(2));
  }


  @Test
  void t3_one_set_scrambled_ok() {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(1));
  }

  @Test
  void t4_two_sets_scrambled_ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, "(");

    await().atMost(ofSeconds(10)).untilAsserted(() ->
        assertThat(outOfOrder.pairs).isEqualTo(2));
  }

  @Test
  void t5_one_closed_may_trigger_infinite_loop_if_resending() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");

    Thread.sleep(1000); // just look in the log
  }

}
