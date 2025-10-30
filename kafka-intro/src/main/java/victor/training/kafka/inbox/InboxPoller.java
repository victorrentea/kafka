package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxPoller {
  private final InboxRepo inboxRepo;
  private final InboxWorker inboxWorker;
  private final ThreadPoolTaskExecutor schedulerWorkers;

  @Scheduled(fixedRate = 500) // debate
  public void processInbox() throws InterruptedException {
    Optional<Inbox> nextTask = inboxRepo.findNext(LocalDateTime.now().minusSeconds(1));
    if (nextTask.isEmpty()) return;
    var task = nextTask.get();
    inboxRepo.save(task.setInProgress());
    // A) release shedlock programatic
    CompletableFuture.runAsync(() -> { // B fire-and-forget/IntegrationFlows..
      try {
        inboxWorker.process(task.getWork()); // sync apel! POATE DURA MULT!
        inboxRepo.save(task.setDone());
      } catch (Exception e) {
        inboxRepo.save(task.setError(e.toString()));
        throw new RuntimeException(e);
      }
    });
  }

}
