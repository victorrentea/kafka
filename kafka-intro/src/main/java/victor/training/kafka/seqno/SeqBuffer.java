package victor.training.kafka.seqno;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Data
public class SeqBuffer {
  record AggIdSeqNo(int aggId, long seqNo) implements Serializable {}

  @Id
  private AggIdSeqNo id;
  private String payload;
  private LocalDateTime insertedAt = LocalDateTime.now();

  public SeqBuffer() {
  }

  public SeqBuffer(AggIdSeqNo id, String payload) {
    this.id = id;
    this.payload = payload;
  }
}
