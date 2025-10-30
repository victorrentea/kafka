package victor.training.kafka.seqno;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.annotation.DirtiesContext;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.seqno.SeqNoListener.SeqMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;
import static victor.training.kafka.seqno.SeqNoListener.*;

@Slf4j
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class SeqNoListenerTest extends KafkaTest {
  public static final int AGG_ID = 1;
  @Autowired
  KafkaTemplate<String, SeqMessage> kafkaTemplate;

  private static final BlockingQueue<String> queue = new LinkedBlockingQueue<>();
  @BeforeEach
  @AfterEach
  final void before() {
      queue.clear();
  }

  @Test
  void resequencesOutOfOrder() throws Exception {
    // send out of order but with seqNo
    kafkaTemplate.send(TOPIC, ""+AGG_ID, new SeqMessage(AGG_ID, 2, "B"));
    kafkaTemplate.send(TOPIC, ""+AGG_ID, new SeqMessage(AGG_ID, 1, "A"));
    kafkaTemplate.send(TOPIC, ""+AGG_ID, new SeqMessage(AGG_ID, 4, "D"));
    kafkaTemplate.send(TOPIC, ""+AGG_ID, new SeqMessage(AGG_ID, 4, "D"));
    kafkaTemplate.send(TOPIC, ""+AGG_ID, new SeqMessage(AGG_ID, 3, "C"));

    await().atMost(ofSeconds(5)).untilAsserted(() ->
        assertThat(queue).containsExactly("A", "B", "C", "D")
    );
  }
  @Test
  void emitPendingMessagesAfterTimeout() throws Exception {
    kafkaTemplate.send(TOPIC, "2", new SeqMessage(2, 1, "A"));
    // gap: no seqNo=2
    kafkaTemplate.send(TOPIC, "2", new SeqMessage(2, 3, "B"));

    await()
        .during(ofSeconds(3)) // holds true for ...
        .atMost(ofSeconds(10)) // within a window of ...
        .untilAsserted(() -> assertThat(queue).containsExactly("A"));
    log.info("Waiting to release old messages .... ");
    await().during(TIME_WINDOW.plus(ofSeconds(1)))
        .untilAsserted(() -> assertThat(queue).containsExactly("A", "B"));
  }

  @TestConfiguration
  @EnableScheduling
  public static class Listener {
    @KafkaListener(topics = OUT_TOPIC, groupId = "test", properties = "auto.offset.reset=latest")
    public void listen(String message) {
      log.info("Received OUT: " + message);
      queue.add(message);
    }
  }
}
