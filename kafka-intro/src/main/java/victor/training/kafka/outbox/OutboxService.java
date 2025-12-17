package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static java.time.Duration.ofMinutes;
import static java.time.LocalDateTime.now;
import static victor.training.kafka.outbox.Outbox.Status.RUNNING;

@Slf4j
@RequiredArgsConstructor
@Service
public class OutboxService {
  private final OutboxRepo outboxRepo;
  private final Sender sender;
  private final InTransaction inTransaction;

  @Service
  @RequiredArgsConstructor
  private static class InTransaction {
    private final OutboxRepo outboxRepo;
    @Transactional
    List<Outbox> selectPendingAndMarkRunning() {
      List<Outbox> pendingList = outboxRepo.findAllPendingAndLockThem();
      for (Outbox outbox : pendingList) {
        outbox.status(RUNNING);
        outbox.runningSince(now());
      }
      return pendingList;
    } // JPA auto-UPDATEs dirty @Entity at @Transaction end
  }

  @Scheduled(fixedRate = 500)
  void sendFromOutbox() {
    var toSend = inTransaction.selectPendingAndMarkRunning();
    for (Outbox outbox : toSend) {
      log.debug("Start outbox {}", outbox);
      try {
        sender.send(outbox.messageToSend());
        outboxRepo.delete(outbox);
        log.debug("OK");
      } catch (Exception e) {
        log.error("Failed to send", e);
      }
    }
  }
  @Scheduled(fixedRate = 1000)
  void resetToPending() {
    log.info("Reset pending for too long");
    var cutoff = now().minus(ofMinutes(5));
    outboxRepo.resetRunningForMoreThan(cutoff);
  }

  // potentially wrapped in caller @Transactional with other changes
  void addToOutbox(String messageToSend) {
    outboxRepo.save(new Outbox().messageToSend(messageToSend));
  }
}
