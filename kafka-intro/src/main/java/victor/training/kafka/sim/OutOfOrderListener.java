package victor.training.kafka.sim;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import victor.training.kafka.JsonUtils;
import victor.training.kafka.inbox.Inbox;
import victor.training.kafka.inbox.InboxRepo;
import victor.training.kafka.sim.SimEvent.AddCredit;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;

import static victor.training.kafka.sim.SimEvent.*;

@Slf4j
@RequiredArgsConstructor
@Service
public class OutOfOrderListener {
  public static final String SIM_TOPIC = "sim-topic";
  private final SimRepo simRepo;
  private final InboxRepo inboxRepo;

  // - producer should emit the second message AFTER it knows that the first was DONE, eg waiting for CreditAddedEvent
  //   => sender of the COMMAND is already coupled to consumer. HOW?
  //   a) block for the event
  //   b) persist some state in between
  //   => will add complexity to sender to wait for my Event

  // - message.key / partitioning / kafka

  // - concurrency="1" ? works in rabbit, not in kafka.
  // Rabbit solution: route messages by a key to 4(eg) queues: sim-1, sim-2, sim-3, sim-4
  //     + connect 1 thread to each of the "sub-queues"

  // - reorder via inbox

  @KafkaListener(topics = SIM_TOPIC)
  public void consume(ConsumerRecord<String, SimEvent> record) throws InterruptedException, JsonProcessingException {
    SimEvent simEvent = record.value();
    String json = JsonUtils.sealedJackson(SimEvent.class).writeValueAsString(simEvent);
    // convert record.timestamp to a LDT
    LocalDateTime observedAt = Instant.ofEpochMilli(record.timestamp())
        .atZone(ZoneId.systemDefault())
        .toLocalDateTime();
    String ik = (simEvent instanceof AddCredit ac) ? ac.ik() : UUID.randomUUID().toString();
    if (inboxRepo.countByIk(ik)==0) {
      inboxRepo.save(new Inbox(json, observedAt, ik));
    }
  }

  @Scheduled(fixedRate = 100)
  public void pollInbox() throws JsonProcessingException, InterruptedException {
    var windowOfResequencer = Duration.ofSeconds(1);
    var notSoonerThan = LocalDateTime.now().minus(windowOfResequencer);
    var opt = inboxRepo.findNext(notSoonerThan);
    if (opt.isEmpty()) return; // no new task
    var nextInbox = opt.get();

    try {
      inboxRepo.save(nextInbox.start());
      log.info("Started on {}", nextInbox);
      var json = nextInbox.getWork();
      SimEvent simEvent = JsonUtils.sealedJackson(SimEvent.class).readValue(json, SimEvent.class);
      process(simEvent);
      // should't I send here to another topic in the correct order
      inboxRepo.save(nextInbox.done());
      log.info("End {}", nextInbox);
    }catch (Exception e) {
      // ? is it correct to later process future messages for this simId?
      // if NO-> adjust the query
      // if YES->
      log.info("Error {}", e, e);
      inboxRepo.save(nextInbox.error(e.getMessage()));
    }
  }

  private void process(SimEvent simEvent) throws InterruptedException {
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
    Thread.sleep(550); // pretend some validations = 10 x 0.5 (default Kafka consumer backoff)
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
