package victor.training.kafka.seqno;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SeqBufferRepo extends JpaRepository<SeqBuffer, Long> {
  long seqNo(long seqNo);
}
