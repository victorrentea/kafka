package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import victor.training.kafka.CheckoutBooks.BookCheckout;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
@Component
@RequiredArgsConstructor
public class Startup {
  private final StreamBridge streamBridge;
  private final KafkaTemplate<String, Event> kafkaTemplate;

//  @EventListener(ApplicationStartedEvent.class)
  @Autowired
  public void onAppStarted(StreamsBuilder builder) {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
//    for (int i = 0; i < 100; i++) {
//      kafkaTemplate.send("myTopic", "a"+i, new Event1("Message" + i));
//    }

//    kafkaTemplate.send("myTopic", "a15", new Event1("FAIL"));

    kafkaTemplate.send("myTopic", "a15", new Event1("M1"));
    kafkaTemplate.send("myTopic", "a56", new Event2("M2"));
    kafkaTemplate.send("myTopic", "a15", new Event1("M3"));

    CompletableFuture.runAsync(() -> {
      kafkaTemplate.send("checkout", "k", CheckoutBooks.builder()
          .eventId(UUID.randomUUID())
          .checkoutId(RandomStringUtils.randomAlphanumeric(8))
          .checkedOutAt(LocalDateTime.now())
          .userId("victor")
          .checkouts(List.of(
              new BookCheckout("6", LocalDate.now().plusDays(30)),
              new BookCheckout("7", LocalDate.now().plusDays(30))
          ))
          .teleportMethod("regular")
          .build());
      log.info("Sent checkout command");
    }, delayedExecutor(1, SECONDS));

  }
}
