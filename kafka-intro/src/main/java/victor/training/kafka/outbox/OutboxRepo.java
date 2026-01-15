package victor.training.kafka.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.*;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxRepo extends JpaRepository<Outbox, Long> {
  String SKIP_LOCKED_ROWS = "-2"; // i.e. don't wait
  @Lock(LockModeType.PESSIMISTIC_WRITE) // SELECT ... FOR UPDATE vs racing instances
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = SKIP_LOCKED_ROWS))
  @Query("""
      select outbox from Outbox outbox
      where outbox.status = 'PENDING'
      """)
  List<Outbox> findAllPendingAndLockThem();

  @Transactional
  @Modifying
  @Query("""
        update Outbox
        set status = 'PENDING',
            runningSince = null
        where status = 'RUNNING'
        and runningSince < :cutoff
        """)
  void resetRunningForMoreThan(LocalDateTime cutoff);
}
