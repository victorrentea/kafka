package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxPoller {
  private final InboxRepo inboxRepo;
  private final InboxWorker inboxWorker;
  private final ThreadPoolTaskExecutor schedulerWorkers;

  @SchedulerLock(name = "processInbox") // to avoid racing instances
  @Scheduled(fixedRate = 500)
  public void processInbox() {
    Optional<Inbox> nextTask = inboxRepo.findNext(LocalDateTime.now().minusSeconds(1));

    if (nextTask.isEmpty()) return; // nothing to do

    var task = nextTask.get();

    // TODO UNDO
    inboxRepo.save(task.setInProgress()); // prevents other instances starting the same task

    try {
//      CompletableFuture.runAsync(() -> {
        // the rest can move to a background thread
        try {
          log.info("Task started {}", task);
          inboxWorker.process(task.getWork());
          log.info("Task completed {}", task);

          inboxRepo.save(task.setDone());
        } catch (Exception e) {
          log.error("Task failed: {}", task, e);
          inboxRepo.save(task.setError(e.getMessage()));
        }
//      }, schedulerWorkers);
    } catch (RejectedExecutionException e) { // all workers busy
      log.error("All workers busy, will retry later for task {}", task);
      inboxRepo.save(task.setPending());
    }
  }


}
