package victor.training.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import reactor.core.publisher.Flux;

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

   @Bean
   public Supplier<Flux<Message<String>>> generator() {
      return () -> Flux.interval(ofSeconds(1))
          .doOnNext(i -> log.info("Emit" + i))
          .map(i -> MessageBuilder.withPayload("msg" + i).setHeader(KafkaHeaders.MESSAGE_KEY, "user").build());
   }

}
