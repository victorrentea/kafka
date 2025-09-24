package victor.training.kafka.seqno;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.seqno.SeqNoListener.SeqMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;
import static victor.training.kafka.seqno.SeqNoListener.OUT_TOPIC;
import static victor.training.kafka.seqno.SeqNoListener.TOPIC;

@Slf4j
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class SeqNoListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, SeqMessage> kafkaTemplate;

  private static final BlockingQueue<String> queue = new LinkedBlockingQueue<>();

  @Test
  void resequencesOutOfOrder() throws Exception {
    // send out of order
    kafkaTemplate.send(TOPIC, new SeqMessage(2, "B"));
    kafkaTemplate.send(TOPIC, new SeqMessage(1, "A"));
    kafkaTemplate.send(TOPIC, new SeqMessage(4, "D"));
    kafkaTemplate.send(TOPIC, new SeqMessage(3, "C"));

    // then
    assertThat(queue.poll(5, TimeUnit.SECONDS)).isEqualTo("A");
    assertThat(queue.poll(2, TimeUnit.SECONDS)).isEqualTo("B");
    assertThat(queue.poll(2, TimeUnit.SECONDS)).isEqualTo("C");
    assertThat(queue.poll(2, TimeUnit.SECONDS)).isEqualTo("D");
  }

  @TestConfiguration
  public static class Listener {
    @KafkaListener(topics = OUT_TOPIC, groupId = "test", properties = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=latest")
    public void listen(String message) {
      log.info("Received OUT: " + message);
      queue.add(message);
    }
  }
}
