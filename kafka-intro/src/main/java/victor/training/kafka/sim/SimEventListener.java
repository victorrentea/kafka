package victor.training.kafka.sim;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Service;
import victor.training.kafka.JsonUtils;
import victor.training.kafka.sim.SimEvent.CreditAdded;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static victor.training.kafka.sim.SimEvent.OfferActivated;

@Slf4j
@RequiredArgsConstructor
@Service
public class SimEventListener {
  public static final String SIM_TOPIC = "sim-topic";
  private final SimRepo simRepo;

  // TODO fix Out of order
  //  1. concurrency="1" => ?
  //  2. same message.key => same partition = consumed in same order, by 1 thread
  //  3. @DelayedRetry
  //  4. [homework exercise] reorder via INBOX table by message timestamp (observedAt by producer)

  @KafkaListener(topics = SIM_TOPIC)
  public void consume(ConsumerRecord<String, SimEvent> record) throws InterruptedException {
    SimEvent simEvent = record.value();
    switch (simEvent) {
      case CreditAdded event -> addCredit(event);
      case OfferActivated event -> activateOffer(event);
    }
  }

  private void addCredit(CreditAdded event) throws InterruptedException {
    var sim = simRepo.findById(event.simId()).orElseThrow();
    Thread.sleep(100); // pretend some validations = 10 x 0.5 (default Kafka consumer backoff)
    sim.credit(sim.credit() + event.credit());
    simRepo.save(sim);
  }

  private void activateOffer(OfferActivated event) {
    var sim = simRepo.findById(event.simId()).orElseThrow();
    if (sim.credit() < event.price()) {
      log.error("Not enough credit for sim {} to activate offer {}", sim.id(), event.offerId());
      throw new IllegalArgumentException("Not enough credit");
    }
    sim.credit(sim.credit() - event.price());
    sim.activeOfferId(event.offerId());
    simRepo.save(sim);
  }


  @Bean
  public NewTopic simTopic() {
    return TopicBuilder.name(SIM_TOPIC).partitions(2).build();
  }
}
