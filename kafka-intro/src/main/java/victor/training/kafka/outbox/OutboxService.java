package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

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

  // potentially wrapped in caller @Transactional with other changes
  void addToOutbox(String messageToSend) {
  }

//  @SchedulerLock// alternative pod-race protection = multiple app instances via some shared DB lock
  @Scheduled(fixedRateString = "${outbox.poll.rate.ms:500}") // 🤔 adds up to 500 ms latency to sending the message
  void sendFromOutbox() {

  }

  @Service
  @RequiredArgsConstructor
  // prevents race conditions when multiple instances of your app are running
  private static class InTransaction {
    private final OutboxRepo outboxRepo;
    @Transactional
    List<Outbox> selectPendingAndMarkRunning() {
      return null;
    }
  }

  @Scheduled(fixedRate = 60000)
  void resetToPending() { // watchdog
    log.info("Reset messages RUNNING since more than Δt => sets their .status=PENDING > reprocess them");
    var cutoff = now().minus(ofMinutes(5));
    outboxRepo.resetRunningForMoreThan(cutoff);
  }
}
