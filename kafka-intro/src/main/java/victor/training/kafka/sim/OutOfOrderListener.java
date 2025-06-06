package victor.training.kafka.sim;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Service;
import victor.training.kafka.JsonUtils;
import victor.training.kafka.sim.SimEvent.AddCredit;

import static victor.training.kafka.sim.SimEvent.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class OutOfOrderListener {
  public static final String SIM_TOPIC = "sim-topic";
  private final SimRepo simRepo;

  // - producer should emit the second message AFTER it knows that the first was DONE, eg waiting for CreditAddedEvent
  //   => sender of the COMMAND is already coupled to consumer. HOW?
  //   a) block for the event
  //   b) persist some state in between
  //   => will add complexity to sender to wait for my Event

  // - message.key / partitioning / kafka

  // - concurrency="1" ? works in rabbit, not in kafka
  // - reorder via inbox

  @KafkaListener(topics = SIM_TOPIC, concurrency = "1")
  public void consume(ConsumerRecord<String, SimEvent> record) throws InterruptedException, JsonProcessingException {
    SimEvent simEvent = record.value();
    var sim = simRepo.findById(simEvent.simId()).orElseThrow();
    switch (simEvent) {
      case AddCredit event -> addCredit(event, sim);
      case ActivateOffer event -> activateOffer(event, sim);
    }
    simRepo.save(sim);
  }

  private void addCredit(AddCredit event, Sim sim) throws InterruptedException {
    apiCallToCheckDebt();
    sim.credit(sim.credit() + event.credit());
  }

  private void apiCallToCheckDebt() throws InterruptedException {
    Thread.sleep(100); // pretend some validations = 10 x 0.5 (default Kafka consumer backoff)
  }

  private void activateOffer(ActivateOffer activateOffer, Sim sim) {
    if (sim.credit() < activateOffer.price()) {
      log.error("Not enough credit for sim {} to activate offer {}", sim.id(), activateOffer.offerId());
      throw new IllegalArgumentException("Not enough credit");
    }
    sim.credit(sim.credit() - activateOffer.price());
    sim.activeOfferId(activateOffer.offerId());
  }

  @Bean
  public NewTopic outOfOrder() {
    return TopicBuilder.name(SIM_TOPIC).partitions(2).build();
  }
}
