package victor.training.kafka.inbox;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface InboxRepo extends JpaRepository<Inbox, Long> {
  List<Inbox> findByStatusAndCreatedAtAfter(Inbox.Status status, LocalDateTime createdSince, PageRequest pageRequest);
}
