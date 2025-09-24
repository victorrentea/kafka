package victor.training.kafka.seqno;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SeqTrackingRepo extends JpaRepository<SeqTracking, Integer> {
}
