package victor.training.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.time.Duration.ofSeconds;

@Slf4j
@SpringBootApplication
public class FunctionsApp {
   public static void main(String[] args) {
      SpringApplication.run(FunctionsApp.class, args);
   }


   @Bean
   public NewTopic pageViews() {
      return new NewTopic("fb-message", 2, /*replication factor*/ (short) 1);
   }

   public static final String[] MESSAGES = {"love", "trump", "bomb", "english", "french"};

   @Bean
   public Supplier<Flux<Message<String>>> generator() {
      System.out.println("Generator created");
      return () -> Flux.interval(ofSeconds(1))
          .doOnNext(i -> log.info("Emit" + i))
          .map(i -> random(MESSAGES))
          .map(text -> MessageBuilder.withPayload(text).setHeader(KafkaHeaders.MESSAGE_KEY, "user").build());
   }

   private String random(String[] arr) {
      return arr[new Random().nextInt(arr.length)];
   }


   @Bean
   public Consumer<KStream<String, String>> printer() {
      return kstream -> kstream.foreach((k, v) -> {
         log.info("Got message: {} - {}", k, v);
      });
   }

   // sends on the out KStream the messages that contain the word "bomb", keyed by their user
   @Bean
   public Function<KStream<String, String>, KStream<String, String>> bombFilter() {
      return kstream -> kstream
          .peek((k, v) -> log.info("BOMB Got message: {} - {}", k, v))
          .filter((k, v) -> v.contains("bomb"));
   }

//   @Bean
//   public Function<KStream<Object, String>, KStream<String, String>[]> wordcount() {
//
//      Predicate<String, String> isEnglish = (k, v) -> k.equals("english");
//      Predicate<String, String> isFrench = (k, v) -> k.equals("french");
//      Predicate<String, String> isSpanish = (k, v) -> k.equals("spanish");
//
//      return input -> input
//          .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
//          .groupBy((key, value) -> value)
//          .windowedBy(TimeWindows.of(5000))
//          .count(Materialized.as("WordCounts-branch"))
//          .toStream()
////          .map((key, value) -> new KeyValue<>(key.key(), value + ""))
////          .branch(isEnglish, isFrench, isSpanish)
//          ;
//   }
}
