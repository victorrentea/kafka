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
import victor.training.kafka.sim.SimEvent.CreditAdded;

import static victor.training.kafka.sim.SimEvent.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class OutOfOrderListener {
  public static final String SIM_TOPIC = "sim-topic";
  private final SimRepo simRepo;

  // - concurrency="1" ?
  // - message.key
  // - reorder via inbox

  @KafkaListener(topics = SIM_TOPIC)
  public void consume(ConsumerRecord<String, SimEvent> record) throws InterruptedException, JsonProcessingException {
    SimEvent simEvent = record.value();
    var sim = simRepo.findById(simEvent.simId()).orElseThrow();
    switch (simEvent) {
      case CreditAdded event -> addCredit(event, sim);
      case OfferActivated event -> activateOffer(event, sim);
    }
    String json = JsonUtils.sealedJackson(SimEvent.class).writeValueAsString(simEvent);
    SimEvent parsed = JsonUtils.sealedJackson(SimEvent.class).readValue(json, SimEvent.class);
    System.out.println("Parsed: " + parsed);
    simRepo.save(sim);
  }

  private void addCredit(CreditAdded event, Sim sim) throws InterruptedException {
    Thread.sleep(100); // pretend some validations = 10 x 0.5 (default Kafka consumer backoff)
    sim.credit(sim.credit() + event.credit());
  }

  private void activateOffer(OfferActivated e, Sim sim) {
    if (sim.credit() < e.price()) {
      log.error("Not enough credit for sim {} to activate offer {}", sim.id(), e.offerId());
      throw new IllegalArgumentException("Not enough credit");
    }
    sim.credit(sim.credit() - e.price());
    sim.activeOfferId(e.offerId());
  }

  @Bean
  public NewTopic outOfOrder() {
    return TopicBuilder.name(SIM_TOPIC).partitions(2).build();
  }
}
