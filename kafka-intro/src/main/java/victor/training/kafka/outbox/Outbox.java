package victor.training.kafka.outbox;

import jakarta.persistence.*;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDateTime;
import java.util.UUID;

import static jakarta.persistence.EnumType.STRING;
import static victor.training.kafka.inbox.Inbox.Status.PENDING;

@Entity
@Data
public class Outbox {
  @Id
  @GeneratedValue
  private Long id;
  private String messageToSend;
  private String error;

}
