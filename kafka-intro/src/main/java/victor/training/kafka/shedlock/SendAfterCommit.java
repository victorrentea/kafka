package victor.training.kafka.shedlock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@RequiredArgsConstructor
@Service
public class SendAfterCommit {
  private final ApplicationEventPublisher applicationEventPublisher;

  @Transactional
  @EventListener(ApplicationStartedEvent.class)
  public void poll() {
    System.out.println("in tx before");
    applicationEventPublisher.publishEvent(new EventToSend());
    System.out.println("in tx after");
  }
  record EventToSend() {}
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(EventToSend ev) {
    System.out.println("In listener: " +ev);
  }
}


