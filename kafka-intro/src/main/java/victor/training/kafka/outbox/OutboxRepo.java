package victor.training.kafka.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.Optional;

public interface OutboxRepo extends JpaRepository<Outbox, Long> {
  // TODO DELETE
  @Query("""
      select outbox
      from Outbox outbox
      where outbox.error = null
      order by outbox.id
      limit 1
      """)
  Optional<Outbox> findNext();
}
