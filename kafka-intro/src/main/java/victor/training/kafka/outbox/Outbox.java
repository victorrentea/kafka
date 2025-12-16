package victor.training.kafka.outbox;

import jakarta.persistence.*;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDateTime;
import java.util.UUID;

import static jakarta.persistence.EnumType.STRING;

@Entity
@Data
class Outbox {
  @Id
  @GeneratedValue
  private Long id;
  private String messageToSend;
  enum Status {PENDING, RUNNING}
  @Enumerated(STRING)
  private Status status = Status.PENDING;
  private LocalDateTime runningSince;
}
