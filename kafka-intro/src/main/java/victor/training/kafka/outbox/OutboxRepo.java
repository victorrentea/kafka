package victor.training.kafka.outbox;

import jakarta.persistence.LockModeType;
import jakarta.persistence.QueryHint;
import org.springframework.data.jpa.repository.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public interface OutboxRepo extends JpaRepository<Outbox, Long> {
  @Query("""
      select outbox
      from Outbox outbox
      where outbox.status = 'PENDING'
      """)
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @QueryHints(@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")) // SKIP LOCKED
  List<Outbox> findAllPendingLockingThem();

  @Modifying
  @Query("""
        update Outbox
        set status = 'PENDING',
            runningSince = null
        where runningSince < :cutoff
        """)
  void resetRunningForMoreThan(Instant cutoff);
}
