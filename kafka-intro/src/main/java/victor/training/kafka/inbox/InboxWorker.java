package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxWorker {
  private final InboxRepo inboxRepo;
  @Scheduled(fixedRate = 500)
  // TODO in real projects avoid racing pods using https://www.baeldung.com/shedlock-spring
  // @SchedulerLock(name = "InboxScheduler", lockAtMostFor = "5m", lockAtLeastFor = "1m")
  public void checkInbox() {
//    log.info("Checking inbox");

    List<Inbox> tasks = inboxRepo.findByStatusAndCreatedAtAfter(
        Inbox.Status.PENDING,
        LocalDateTime.now().minusMinutes(1),
        PageRequest.of(0, 1, Sort.by(Sort.Order.asc("messageTimestamp"))));

    if (tasks.isEmpty()) {
      log.trace("No tasks found");
      return;
    }
    var task = tasks.getFirst();
    log.info("Processing task: {}", task);
    task.start();
    inboxRepo.save(task);
    try {
      Thread.sleep(1000);
      if (Math.random() < 0.1) {
        throw new RuntimeException("Error during processing (pretend)");
      }
      task.done();
      inboxRepo.save(task);
      log.info("Normal completion of task: {}", task);
    } catch (Exception e) {
      log.error("Error processing task: {}", task, e);
      task.error(e.getMessage());
      inboxRepo.save(task);
    }
  }
}
