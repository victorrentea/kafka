package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxPoller {
  private final InboxRepo inboxRepo;
  @Value("${inbox.time.window.ms}")
  private final Integer inboxTimeWindowMillis;
  private final InboxWorker inboxWorker;

//   @SchedulerLock //TODO to avoid racing pods: https://www.baeldung.com/shedlock-spring
  @Scheduled(fixedRate = 500)
  public void processFromInbox() {
    Optional<Inbox> taskOptional = inboxRepo.findNext(
        LocalDateTime.now().minus(Duration.ofMillis(inboxTimeWindowMillis)));

    if (taskOptional.isEmpty()) return;

    var task = taskOptional.get();
    log.trace("Start task {}",task);

    // TODO UNDO
    inboxRepo.save(task.start());
    // from here move to a background thread
    try {
      inboxWorker.process(task.getWork());

      inboxRepo.save(task.done());
    } catch (Exception e) {
      log.error("Task failed: {}", task, e);
      inboxRepo.save(task.error(e.getMessage()));
    }
  }


}
