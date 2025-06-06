package victor.training.kafka.inbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.Optional;

public interface InboxRepo extends JpaRepository<Inbox, Long> {
  // TODO DELETE
  @Query("""
      select inbox
      from Inbox inbox
      where inbox.status = 'PENDING'
        and inbox.receivedAt < :createdUntil
      order by inbox.messageTimestamp
      limit 1
      """)
//            order by CASE WHEN eventtype='add' THEN 0 WHEN eventtype='update' THEN 1 ELSE 2 END, inbox.messageTimestamp
  Optional<Inbox> findNext(LocalDateTime createdUntil);

  int countByIk(String ik);
}
