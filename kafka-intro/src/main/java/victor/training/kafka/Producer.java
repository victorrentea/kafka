package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logging.MDC;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
@RequiredArgsConstructor
@Profile("!test")
public class Producer {
  @EventListener(ApplicationStartedEvent.class)
  public void onStartup() {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
    produceEvent();
  }

  private final KafkaTemplate<String, Event> kafkaTemplate;

  @GetMapping("produce")
  public void produceEvent() {
    MDC.put("traceId", "123"); // pretend setup by (a) an HTTP filter or (b) a Kafka Listener interceptor
    Event event = new Event.EventOK("Hello Kafka!");
    var future = kafkaTemplate.send("myTopic", "spring", event);
    // #2 fire-and-forget
    //    future.get();// #1 blochez threadul pana mesajul ajunge la Broker daca e mesaj critic
       // ? la shutdown spring se trimit cele ramase pe tzeava

    // #3 async
    future.thenAccept(result -> {
      log.info("Message sent to partition " + result.getRecordMetadata().partition() + " cu offset "
        + result.getRecordMetadata().offset());
    });

    log.info("Messages sent");
  }
}
