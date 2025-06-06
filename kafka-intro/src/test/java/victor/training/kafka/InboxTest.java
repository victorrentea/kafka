package victor.training.kafka;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.Event.EventForLater;
import victor.training.kafka.inbox.InboxWorker;

import java.util.Date;
import java.util.UUID;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;

@ActiveProfiles("test")
@SpringBootTest
class InboxTest {
  @Autowired
  KafkaTemplate<String, Event> kafkaTemplate;
  @MockitoBean
  InboxWorker worker;

  @Test
  void eliminatesDuplicates() throws InterruptedException {
    UUID idempotencyKey = UUID.randomUUID();
    EventForLater event = new EventForLater("work", idempotencyKey);
    kafkaTemplate.send("myTopic", "k", event);
    kafkaTemplate.send("myTopic", "k", event); // = producer/broker retry

    Thread.sleep(3000);
    verify(worker, Mockito.times(1)).process("work");
  }

  @Test
  void reordersMessagesByTimestamp() throws InterruptedException {
    var t0 = new Date().getTime() - 100;
    var t1 = new Date().getTime();

    kafkaTemplate.send("myTopic", 1,
        t1,
        "k", new EventForLater("work2", UUID.randomUUID()));
    kafkaTemplate.send("myTopic", 1,
        t0,
        "k", new EventForLater("work1", UUID.randomUUID()));

    Thread.sleep(3000);

    InOrder inOrder = inOrder(worker);
    inOrder.verify(worker).process("work1");
    inOrder.verify(worker).process("work2");
  }

}
