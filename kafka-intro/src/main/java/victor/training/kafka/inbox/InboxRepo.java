//package victor.training.kafka.inbox;
//
//import org.springframework.data.jpa.repository.JpaRepository;
//import org.springframework.data.jpa.repository.Query;
//
//import java.time.LocalDateTime;
//import java.util.Optional;
//
//public interface InboxRepo extends JpaRepository<Inbox, Long> {
//  @Query("""
//      select inbox
//      from Inbox inbox
//      where inbox.status = 'PENDING'
//        and inbox.receivedAt < now() - 5000
//      order by inbox.observedAt
//      limit 1
//      """) // runs on a scheduler, eg 1/sec
//  Optional<Inbox> findNext(LocalDateTime createdUntil);
//}
