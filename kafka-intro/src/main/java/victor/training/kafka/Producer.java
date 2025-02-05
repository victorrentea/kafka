package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logging.MDC;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import victor.training.kafka.Event.EventTakingLong;

import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("!test")
public class Producer {
  private final KafkaTemplate<String, Event> kafkaTemplate;

  @EventListener(ApplicationStartedEvent.class)
  public void onAppStarted() throws Exception {
    MDC.put("traceId", "123"); // normally setup by (a) an HTTP filter or (b) a Kafka Listener interceptor
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
    kafkaTemplate.send("myTopic", "1", new Event.EventOK("M1"));
    // TODO send sync/async/fire-and-forget
    // TODO extract offset of sent message
    // TODO cause delay/error on consumer
    // TODO propagate traceId
    log.info("Messages sent");
  }
}
