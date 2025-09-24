package victor.training.kafka.inbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.Optional;

public interface InboxRepo extends JpaRepository<Inbox, Long> {
  @Query("""
      select inbox
      from Inbox inbox
      where inbox.status = 'PENDING'
      order by inbox.priority asc
      limit 1
      """)
  Optional<Inbox> findNext();
}
