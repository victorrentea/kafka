package victor.training.kafka.seqno;

import org.springframework.data.jpa.repository.JpaRepository;

public interface NextSeqRepo extends JpaRepository<NextSeq, Integer> {
}
