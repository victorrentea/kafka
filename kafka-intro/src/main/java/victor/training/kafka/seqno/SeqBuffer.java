package victor.training.kafka.seqno;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeqBuffer {
  @Id
  private long seqNo;
  private String payload;

  // Explicit getter to ensure availability even if Lombok annotation processing is not active
  public String getPayload() {
    return payload;
  }
}
