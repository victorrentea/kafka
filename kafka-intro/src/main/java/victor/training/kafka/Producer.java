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

@Slf4j
@RestController
@RequiredArgsConstructor
@Profile("!test")
public class Producer {
  private final KafkaTemplate<String, Event> kafkaTemplate;

  @EventListener(ApplicationStartedEvent.class)
  public void onStartup() {
    produceEvent();
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
  }

  @GetMapping("produce")
  public void produceEvent() {
    MDC.put("traceId", "123"); // pretend setup by (a) an HTTP filter or (b) a Kafka Listener interceptor
    kafkaTemplate.send("myTopic", "1", new Event.EventOK("M1"));
    // TODO send sync/async/fire-and-forget
    // TODO extract offset of sent message
    // TODO cause delay/error on consumer
    // TODO propagate traceId
    log.info("Messages sent");
  }
}
