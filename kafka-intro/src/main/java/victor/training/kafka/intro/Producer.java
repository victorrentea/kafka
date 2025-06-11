package victor.training.kafka.intro;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logging.MDC;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
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
    MDC.put("traceId", "123"); // pretend setup by (a) an HTTP filter or (b) a Kafka Listener interceptor
    // TODO send sync/async/fire-and-forget
    kafkaTemplate.send("myTopic", new Event.EventOK("Work to be done"))
        .thenAccept(r-> {
          sleep(100000);// blocking the SINGLE ONE THREAD of the kafka producer
        });
    // TODO extract offset of sent message
    kafkaTemplate.send("myTopic", new Event.EventOK("Work to be done"))
        .thenAcceptAsync(result -> log.info("Sent message offset: " + result.getRecordMetadata().offset()));
    log.info("Messages sent");
  }
}
