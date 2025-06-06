package victor.training.kafka.inbox;

import jakarta.persistence.*;
import lombok.ToString;

import java.time.LocalDateTime;

import static jakarta.persistence.EnumType.STRING;
import static victor.training.kafka.inbox.Inbox.Status.PENDING;

@ToString
@Entity
@Table(uniqueConstraints = @UniqueConstraint(columnNames = "idempotency_key")) // TODO undo
public class Inbox {
  @Id
  @GeneratedValue
  private Long id;
  private String work;

  private String idempotencyKey;

  @Enumerated(STRING)
  private Status status = PENDING;
  public enum Status {
    PENDING, IN_PROGRESS, DONE, ERROR
  }
  private String error;

  private LocalDateTime observedAt;
  private LocalDateTime receivedAt = LocalDateTime.now();
  private LocalDateTime startedAt;
  private LocalDateTime completedAt;

  protected Inbox() {} // for Hibernate only

  public Inbox(String workJson, LocalDateTime observedAt, String idempotencyKey) {
    this.work = workJson;
    this.observedAt = observedAt;
    this.idempotencyKey = idempotencyKey;
  }

  public String getWork() {
    return work;
  }

  public Long getId() {
    return id;
  }

  public Inbox start() {
    if(status != PENDING) {
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
    completedAt = LocalDateTime.now();
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
