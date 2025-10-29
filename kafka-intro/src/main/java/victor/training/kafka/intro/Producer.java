package victor.training.kafka.intro;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jboss.logging.MDC;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@Profile("!test")
public class Producer {

//  @EventListener(ApplicationStartedEvent.class)
//  @Transactional(transactionManager = "kafkaTransactionManager")
  public void onStartup() {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
    produceEvent();
  }

  private final KafkaTemplate<String, Event> kafkaTemplate;

  @GetMapping("produce")
  public void produceEvent() {
    MDC.put("traceId", "123");
    kafkaTemplate.send("myTopic", new Event.EventOK("Work to be done"))
        .thenAccept(result -> {
          log.info("Trimis mesaj cu offset< ruleaza in threadul unic al producerului din app asta: " + result.getRecordMetadata().offset());
//          db.insert()// network call 5ms => throughput max = 200 m/s = RAU!
         });
    log.info("Messages sent ?!?! sigur");
  }
}
