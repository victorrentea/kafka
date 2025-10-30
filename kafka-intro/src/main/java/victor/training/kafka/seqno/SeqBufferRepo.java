package victor.training.kafka.seqno;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface SeqBufferRepo extends JpaRepository<SeqBuffer, SeqBuffer.AggIdSeqNo> {
  List<SeqBuffer> findSeqBufferByInsertedAtBefore(LocalDateTime time);
}
