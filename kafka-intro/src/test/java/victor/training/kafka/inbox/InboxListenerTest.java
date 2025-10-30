package victor.training.kafka.inbox;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.inbox.InboxListener.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.inbox.InboxListener.TOPIC;

@Slf4j
public class InboxListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, Message> kafkaTemplate;
  @Autowired
  InboxWorker inboxWorker;


  @Test
  void ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC,"key", new Message("-5E","ik1"));
    //send duplicate
    kafkaTemplate.send(TOPIC,"key", new Message("-5E","ik1"));
    //send a higher priority message
    kafkaTemplate.send(TOPIC,"key", new Message("+5E","ik2"));

    Thread.sleep(2000);
    assertThat(inboxWorker.completedWork).containsExactly("+5E", "-5E");
  }

}
