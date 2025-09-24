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
    LocalDateTime aWhileAgo = LocalDateTime.now().minus(Duration.ofMillis(inboxTimeWindowMillis));
    Optional<Inbox> taskOptional = inboxRepo.findNext();

    if (taskOptional.isEmpty()) return;

    var taskJson = taskOptional.get();
    log.trace("Start task {}",taskJson);

    // TODO UNDO
    inboxRepo.save(taskJson.start()); // prevents other instances racing with me
    // from here move to a background thread
    try {
      inboxWorker.process(taskJson.getWork());

      inboxRepo.save(taskJson.done());
    } catch (Exception e) {
      log.error("Task failed: {}", taskJson, e);
      inboxRepo.save(taskJson.error(e.getMessage()));
    }
  }


}
