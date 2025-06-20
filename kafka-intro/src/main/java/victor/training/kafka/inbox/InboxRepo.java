package victor.training.kafka.inbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.Optional;

public interface InboxRepo extends JpaRepository<Inbox, Long> {
  // TODO DELETE
  // @Rabbit/KafkaListener only inserts
  @Query("""
      select inbox
      from Inbox inbox
      where inbox.status = 'PENDING'
        and inbox.receivedAt < now() - 5000 /*=insert date => +latency #bad*/
      order by inbox.observedAt /*user action timestamp*/
      limit 1
      """) // runs on a scheduler, eg 1/sec
  Optional<Inbox> findNext(LocalDateTime createdUntil);
}
