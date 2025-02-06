package victor.training.kafka.inbox;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface InboxRepo extends JpaRepository<Inbox, Long> {
  @Query("""
      select inbox
      from Inbox inbox
      """)
  Optional<Inbox> findNextTask(LocalDateTime createdSince);
}
