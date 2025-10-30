package victor.training.kafka.shedlock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@RequiredArgsConstructor
@Service
public class SendAfterCommit {
  private final ApplicationEventPublisher applicationEventPublisher;

//  @Scheduled(fixedRate = 1000)
  @Transactional
  public void poll() {
    System.out.println("in tx before");
    applicationEventPublisher.publishEvent(new Ev());
    System.out.println("in tx after");
  }
  record Ev() {}
  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void on(Ev ev) {
    System.out.println("In listener: " +ev);
  }
}


