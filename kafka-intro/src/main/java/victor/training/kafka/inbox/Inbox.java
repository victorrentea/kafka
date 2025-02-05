package victor.training.kafka.inbox;

import jakarta.persistence.*;
import lombok.Data;
import lombok.ToString;

import java.time.LocalDateTime;

@ToString
@Entity
public class Inbox {
  @Id
  @GeneratedValue
  private Long id;
  private String work;
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
  public Inbox(String work, LocalDateTime messageTimestamp) {
    this.work = work;
    this.messageTimestamp = messageTimestamp;
  }

  public Long getId() {
    return id;
  }

  public void start() {
    if(status != Status.PENDING) {
      throw new IllegalStateException("Can't start if not PENDING");
    }
    status = Status.IN_PROGRESS;
    startedAt = LocalDateTime.now();
  }

  public void done() {
    if(status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as DONE if not IN_PROGRESS");
    }
    status = Status.DONE;
  }

  public void error(String error) {
    if (status != Status.IN_PROGRESS) {
      throw new IllegalStateException("Can't mark as ERROR if not IN_PROGRESS");
    }
    status = Status.ERROR;
    this.error = error;
  }

}
