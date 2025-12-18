package victor.training.kafka.test.sender;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.testutil.SentKafkaRecord;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@Import(TestedSenderLeakingTest.TestListener.class)
public class TestedSenderLeakingTest extends IntegrationTest {
  @Autowired
  TestedSender testedSender;
  @MockitoBean
  BService bService;
  @Autowired
  TestListener testListener;
  String message = UUID.randomUUID().toString();

  static class TestListener {
    private LinkedBlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();

    public String takeMessage(long timeout, TimeUnit unit) throws InterruptedException {
      return receivedMessages.poll(timeout, unit);
    }

    @KafkaListener(topics = TestedSender.OUT_TOPIC,
        groupId = "TestedSenderLeakingTest",
        properties = {"auto.offset.reset=latest"})
    void logic(String message) {
      receivedMessages.add(message);
    }
  }

  @Test
  void calls() {
    testedSender.send(message);
    verify(bService).logic(message);
  }

  @Test
  void sends() throws Exception {
    testedSender.send(message);
    var record = testListener.takeMessage(1, TimeUnit.SECONDS);
    assertThat(record).isEqualTo(message);
  }
}
