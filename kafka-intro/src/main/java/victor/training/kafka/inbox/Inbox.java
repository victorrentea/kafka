package victor.training.kafka.inbox;

import jakarta.persistence.*;
import lombok.ToString;

import java.time.LocalDateTime;
import java.util.UUID;

@ToString
@Entity
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"idempotency_key"})) // TODO delete
public class Inbox {
  @Id
  @GeneratedValue
  private Long id;
  private String work;
  private UUID idempotencyKey;
  @Enumerated(EnumType.STRING)
  private Status status = Status.PENDING;
  public enum Status {
    PENDING, IN_PROGRESS, DONE, ERROR
  }
  private String error;
  private LocalDateTime messageTimestamp;
  private LocalDateTime createdAt = LocalDateTime.now();
  private LocalDateTime startedAt;

  protected Inbox() {} // for Hibernate only
  public Inbox(String work, LocalDateTime messageTimestamp, UUID idempotencyKey) {
    this.work = work;
    this.messageTimestamp = messageTimestamp;
    this.idempotencyKey = idempotencyKey;
  }

  public String getWork() {
    return work;
  }

  public Long getId() {
    return id;
  }

  public Inbox start() {
    if(status != Status.PENDING) {
      throw new IllegalStateException("Can't start if not PENDING");
    }
    status = Status.IN_PROGRESS;
    startedAt = LocalDateTime.now();
    return this;
  }

  public Inbox done() {
    if(status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as DONE if not IN_PROGRESS");
    }
    status = Status.DONE;
    return this;
  }

  public Inbox error(String error) {
    if (status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as ERROR if not IN_PROGRESS");
    }
    status = Status.ERROR;
    this.error = error;
    return this;
  }

}
