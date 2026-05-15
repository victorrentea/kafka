package victor.training.kafka.outbox;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.web.bind.annotation.*;
import victor.training.kafka.sim.Sim;
import victor.training.kafka.sim.SimRepo;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@RestController
public class SimActiveApi {
  private final Sender sender;
  private final SimRepo simRepo;
  private final OutboxService outboxService;
  private final ApplicationEventPublisher events;

  @PostConstruct
  public void insertSim1() {
    if (simRepo.count() == 0) {
      var sim = new Sim();
      simRepo.save(sim);
    }
  }


  @GetMapping("/sim/{id}/active") // http://localhost:8080/sim/1/active
  @Transactional
  public void setActive(@PathVariable Long id) throws InterruptedException {
    var sim = simRepo.findById(id).orElseThrow();
    sim.active(!sim.active());
    simRepo.save(sim);
//    sender.send("Event", UUID.randomUUID()); // FIXME: make robust (at-least-once)
    outboxService.addToOutbox("Event$$$$$$$${mydata}json");
    events.publishEvent(new OutboxAppendedEvent());
    log.info("Exiting the method");
  }

  public record OutboxAppendedEvent() {}

  @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
  public void flushOutboxAfterCommit(OutboxAppendedEvent event) throws InterruptedException {
    outboxService.sendFromOutbox();
  }
}
