package victor.training.kafka.inbox;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.inbox.InboxListener.Message;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import java.util.UUID;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static victor.training.kafka.inbox.InboxListener.TOPIC;

@Slf4j
@ResetKafkaOffsets(TOPIC)
public class InboxListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, Message> kafkaTemplate;
  @Autowired
  InboxWorker inboxWorker;

  private static final String runId = UUID.randomUUID().toString();

  @Test
  void ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC,"key", new Message("-5E","ik1-"+runId));
    //send duplicate
    kafkaTemplate.send(TOPIC,"key", new Message("-5E","ik1-"+runId));
    //send a higher priority message
    kafkaTemplate.send(TOPIC,"key", new Message("+5E","ik2-"+runId));

    await().atMost(ofSeconds(5)).untilAsserted(() ->
            assertThat(inboxWorker.completedWork).containsExactly("+5E", "-5E"));
  }

}
