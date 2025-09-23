package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;

import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.ooo.OutOfOrderDelayRetryListener.TOPIC;

@Slf4j
public class SeqNoListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private OutOfOrderDelayRetryListener outOfOrderListener;

  @BeforeEach
  final void before() {
      outOfOrderListener.pairs = 0;
      outOfOrderListener.open = 0;
  }
  @Test
  void ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(1);
  }
  @Test
  void scrambled() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(1);
  }
  @Test
  void scrambled2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, "(");

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(2);
  }

}
