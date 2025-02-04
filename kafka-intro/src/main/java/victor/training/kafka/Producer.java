package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.jboss.logging.MDC;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import victor.training.kafka.Event.Event1;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("!test")
public class Producer {
  private final KafkaTemplate<String, Event> kafkaTemplate;

  @EventListener(ApplicationStartedEvent.class)
  public void onAppStarted() throws ExecutionException, InterruptedException {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");

    log.info("Sending test messages");
    MDC.put("traceId", "123"); // normally setup by (a) an HTTP filter or (b) a Kafka Listener interceptor
    kafkaTemplate.send("myTopic", "1", new Event1("M1")) // send with callback
        .thenAccept(result -> log.info("Sent M1 with offset: " + result.getRecordMetadata().offset()));
//    kafkaTemplate.send("myTopic", "1", new Event1("FAIL")); // fire-and-forget
//    kafkaTemplate.send("myTopic", "1", new Event1("SLOW"));
    kafkaTemplate.send("myTopic", "1", new Event1("M2")).get(); // sync send
    log.info("Messages sent");


  }
}
