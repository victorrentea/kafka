package victor.training.kafka.seqno;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class NextSeq {
  @Id
  private int aggId;
  private long nextSeqNo = 1;

}
