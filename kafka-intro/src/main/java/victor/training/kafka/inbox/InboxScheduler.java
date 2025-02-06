package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxScheduler {
  private final InboxRepo inboxRepo;
  private final InboxWorker inboxWorker;
  @Value("${inbox.time.window.ms}")
  private final Integer inboxTimeWindowMillis;

  // @SchedulerLock TODO to avoid racing pods: https://www.baeldung.com/shedlock-spring
  @Scheduled(fixedRate = 500)
  public void checkInbox() {
    Optional<Inbox> taskOptional = inboxRepo.findNextTask(
        LocalDateTime.now().minus(Duration.ofMillis(inboxTimeWindowMillis)));

    // TODO DELETE below
    if (taskOptional.isEmpty()) {
      log.trace("No tasks to run");
      return;
    }
    var task = taskOptional.get();

    inboxRepo.save(task.start());
    // from here move to a background thread
    try {
      inboxWorker.process(task.getWork());

      inboxRepo.save(task.done());
    } catch (Exception e) {
      log.error("Error processing task: {}", task, e);
      inboxRepo.save(task.error(e.getMessage()));
    }
  }
}
