package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class OutboxPoller {
  private final OutboxRepo outboxRepo;
  private final Sender sender;

  // @SchedulerLock //TODO to avoid racing pods: https://www.baeldung.com/shedlock-spring
  @Scheduled(fixedRate = 500)
  public void sendFromOutbox() throws InterruptedException {
    Optional<Outbox> optional = outboxRepo.findNext();
    if (optional.isEmpty()) return;
    var outbox = optional.get();

    log.trace("Start task {}",outbox);
    // TODO UNDO
    try {
      sender.send(outbox.messageToSend());
      outboxRepo.delete(outbox);
    } catch (Exception e) {
      log.error("Send failed: {}", outbox, e);
      outboxRepo.save(outbox.error(e.getMessage()));
    }
  }


}
