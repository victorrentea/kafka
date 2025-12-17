package victor.training.kafka.outbox;

import org.LatencyUtils.TimeServices;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.testutil.TimeExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class OutboxTest extends IntegrationTest {
  @RegisterExtension
  TimeExtension timeExtension = new TimeExtension();
  @MockitoBean
  Sender senderMock;
  @Autowired
  private OutboxService outboxService;
  @Autowired
  private OutboxRepo outboxRepo;

  @BeforeEach
  final void before() {
      outboxRepo.deleteAll();
  }

  @Test
  void oneOkViaScheduler() throws InterruptedException {
    outboxService.addToOutbox("M1");

    verify(senderMock, timeout(1000)).send("M1");
  }

  @Test
  void twoOk() throws InterruptedException {
    outboxService.addToOutbox("M1");
    outboxService.addToOutbox("M2");
    outboxService.sendFromOutbox();
    outboxService.sendFromOutbox(); // should do nothing

    verify(senderMock).send("M1");
    verify(senderMock).send("M2");
  }
  @Test
  void oneErrorRetried() throws InterruptedException {
    outboxService.addToOutbox("M1");
    doThrow(new RuntimeException("BOOM")).when(senderMock).send("M1");
    outboxService.sendFromOutbox();
    timeExtension.advanceTime(Duration.ofMinutes(6));
    outboxService.resetToPending();
    reset(senderMock);
    outboxService.sendFromOutbox();
    verify(senderMock).send("M1");
  }

  @Test
  void noRace() throws InterruptedException {
    for (int i = 0; i < 10; i++) {
      CompletableFuture.runAsync(() -> {
        for (int j = 0; j < 1000; j++) {
          outboxService.sendFromOutbox();
        }
      });
    }
    final int N_MESSAGES = 100;
    List<String> messages = IntStream.range(0, N_MESSAGES).mapToObj(i -> "M" + i).toList();
    for (String message : messages) {
      outboxService.addToOutbox(message);
    }

    TimeServices.sleepMsecs(2000);

    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(senderMock,times(N_MESSAGES)).send(captor.capture());
    assertThat(captor.getAllValues()).containsExactlyInAnyOrderElementsOf(messages);
  }
}