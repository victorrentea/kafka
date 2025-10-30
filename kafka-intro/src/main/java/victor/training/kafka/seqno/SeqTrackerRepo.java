package victor.training.kafka.seqno;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SeqTrackerRepo extends JpaRepository<SeqTracker, Integer> {
}
