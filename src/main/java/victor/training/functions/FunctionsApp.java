package victor.training.functions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.time.LocalDateTime.now;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent {
   String page;
   String referer;
   Integer visitSeconds;
   String user;
   String timestamp;
}


@Slf4j
@SpringBootApplication
public class FunctionsApp {
   public static void main(String[] args) {
      SpringApplication.run(FunctionsApp.class, args);
   }

//   @Bean
//   public NewTopic pageViews() {
//      return new NewTopic("fb-message", 2, /*replication factor*/ (short) 1);
//   }

   public static final String[] PAGE = {"training/jpa", "training/spring", "training/reactive", "training/performance"};
   public static final String[] REFERER = {"google.com", "victorrentea.ro", "stackoverflow.com"};
   public static final String[] USER = {"user1", "user2", "user3"};
   public static final  Random random = new Random();

   private String random(String[] arr) {
      return arr[random.nextInt(arr.length)];
   }
   private int randomTime() {
      return 1 + random.nextInt(20);
   }

   private PageViewEvent generateRandomEvent() {
      return new PageViewEvent(random(PAGE), random(REFERER), 1 + random.nextInt(20), random(USER), now().toString());
   }

   @Bean
   public Supplier<Flux<Message<PageViewEvent>>> generator() {
      System.out.println("Generator created");
      return () -> Flux.interval(ofMillis(500))
          .map(i -> generateRandomEvent())
          .map(event -> MessageBuilder.withPayload(event)
              .setHeader(KafkaHeaders.MESSAGE_KEY, event.timestamp)
              .build())
          .doOnNext(event -> log.info("Emit " + event))
          ;
   }

//
   @Bean
   public Consumer<KStream<String, String>> printer() {
      return kstream -> kstream.foreach((k, v) -> {
         log.info("PRINTER: {} - {}", k, v);
      });
   }
//
//   // sends on the out KStream the messages that contain the word "bomb", keyed by their user
   @Bean
   public Function<KStream<String, PageViewEvent>, KStream<String, String>> externalRefererPage() {
      return kstream -> kstream
          .filter((k, v) -> !v.referer.equals("victorrentea.ro"))
//          .peek((k, v) -> log.info("External referer: " + v.referer))
          .mapValues(PageViewEvent::getPage);
   }

   @Bean
   public Function<KStream<String, PageViewEvent>, KStream<String, PageViewEvent>[]> externalRefererSplitter() {
      return kstream -> kstream
          .filter((k,v) -> v.visitSeconds > 3)
          .branch(
              (k, v) -> !v.referer.equals("victorrentea.ro"),
              (k, v) -> v.referer.equals("victorrentea.ro")
          );
   }
//
   @Bean
   public Function<KStream<String, PageViewEvent>, KStream<String, String>> aggregator() {
      return pageViews -> pageViews
          .peek((key, value) -> log.info("Aggregating {}", value))
          .map((key, pageViewEvent) -> new KeyValue<>(pageViewEvent.page, "unused"))
          .groupByKey(Grouped.with(new StringSerde(), new StringSerde()))
          .count(Materialized.as("page-counts"))
          .mapValues(l -> l + "")
          .toStream()
//          .peek((k,v) -> log.info("OUTPUT page views counts: {} = {}", k, v))
          ;
   }

}
