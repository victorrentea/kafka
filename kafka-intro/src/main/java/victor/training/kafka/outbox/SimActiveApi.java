package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.sim.SimRepo;

@Slf4j
@RequiredArgsConstructor
@RestController
public class SimActiveApi {
  private final Sender sender;
  private final SimRepo simRepo;

  @PutMapping("/sim/{id}/active")
  @Transactional
  public void setActive(@PathVariable Long id, @RequestBody Boolean active) throws InterruptedException {
    var sim = simRepo.findById(id).orElseThrow();
    sim.active(active);
    sender.send("Event"); // FIXME: make robust (at-least-once)
  }
}
