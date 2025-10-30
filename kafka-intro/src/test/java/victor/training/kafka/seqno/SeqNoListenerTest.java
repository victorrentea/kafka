package victor.training.kafka.seqno;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.annotation.DirtiesContext;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;
import static victor.training.kafka.seqno.SeqNoListener.*;

@Slf4j
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@ResetKafkaOffsets(IN_TOPIC)
public class SeqNoListenerTest extends IntegrationTest {
  public final int AGG_ID = new Random().nextInt();
  @Autowired
  KafkaTemplate<String, SeqMessage> kafkaTemplate;

  private static final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

  @BeforeEach
  @AfterEach
  final void before() {
    receivedMessages.clear();
  }

  @Test
  void resequencesOutOfOrder() throws Exception {
    // send out of order but with seqNo
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 2, "B"));
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 1, "A"));
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 4, "D"));
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 4, "D"));
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 3, "C"));

    await().atMost(ofSeconds(5)).untilAsserted(() ->
        assertThat(receivedMessages).containsExactly("A", "B", "C", "D")
    );
  }

  @Test
  @Disabled
  void emitPendingMessagesAfterTimeout() throws Exception {
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 1, "A"));
    kafkaTemplate.send(IN_TOPIC, "" + AGG_ID, new SeqMessage(AGG_ID, 3, "B"));

    await()
        .during(ofSeconds(3)) // holds true for ...
        .atMost(ofSeconds(10)) // within a window of ...
        .untilAsserted(() -> assertThat(receivedMessages).containsExactly("A"));
    log.info("Waiting to release old messages .... ");
    await().during(TIME_WINDOW.plus(ofSeconds(1)))
        .untilAsserted(() -> assertThat(receivedMessages).containsExactly("A", "B"));
  }

  @TestConfiguration
  @EnableScheduling
  public static class Listener {
    @KafkaListener(topics = OUT_TOPIC, groupId = "test", properties = "auto.offset.reset=latest")
    public void listen(String message) {
      log.info("Received OUT: " + message);
      receivedMessages.add(message);
    }
  }
}
