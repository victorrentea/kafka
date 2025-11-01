package victor.training.kafka.inbox;

import jakarta.persistence.*;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

import static jakarta.persistence.EnumType.STRING;
import static victor.training.kafka.inbox.Inbox.Status.ERROR;
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

  @Setter
  private Integer priority = 1;
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

  public Inbox setInProgress() {
    if(status != PENDING && status!=ERROR) {
      throw new IllegalStateException("Can't start from " + status);
    }
    status = Status.IN_PROGRESS;
    startedAt = LocalDateTime.now(); // raise alarms for IN_PROGRESS started < 24h ago (eg)
    return this;
  }

  public Inbox setDone() {
    if(status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as DONE unless IN_PROGRESS");
    }
    status = Status.DONE;
    completedAt = LocalDateTime.now();
    return this;
  }

  public Inbox setError(String error) {
    if (status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as ERROR if not IN_PROGRESS");
    }
    status = Status.ERROR;
    this.error = error;
    completedAt = LocalDateTime.now();
    return this;
  }

}
