package victor.training.kafka.shedlock;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Random;

@Slf4j
@RequiredArgsConstructor
//@Service // enable on demand (pollutes log)
public class Schedlock {
  private final JobEventRepo jobEventRepo;
  private int counter=new Random().nextInt(100);

  @Scheduled(fixedRate = 1000)
  @SchedulerLock(name = "poll") // to avoid racing instances
  public void poll() throws InterruptedException {
    counter++;
    log.info("JOB-{}",counter);
    jobEventRepo.save(new JobEvent("Start JOB-"+counter));
    Thread.sleep(1000);
    jobEventRepo.save(new JobEvent("End   JOB-"+counter));
  }
}

interface JobEventRepo extends JpaRepository<JobEvent, Long> {
}
@Entity
@Data
class JobEvent {
  @GeneratedValue
  @Id
  Long id;
  Long pid = ProcessHandle.current().pid();;
  String event;
  LocalDateTime time = LocalDateTime.now();

  public JobEvent(String event) {
    this.event = event;
  }
}