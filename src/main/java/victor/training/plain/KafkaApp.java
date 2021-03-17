package victor.training.plain;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static java.time.Duration.ofSeconds;

@Slf4j
@RequiredArgsConstructor
@SpringBootApplication
public class KafkaApp {
   public static void main(String[] args) {
      SpringApplication.run(KafkaApp.class, args);
   }

   @Bean
   public NewTopic pageViews() {
      return new NewTopic("page-views", 10, /*replication factor*/ (short) 1);
   }


}


@Slf4j
@RequiredArgsConstructor
@Component
class KafkaSubscriber {

   private Disposable subscribe;

   @PostConstruct
   public void startSubscriber() {
      Map<String, Object> props = new HashMap<>();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(props)
          .subscription(Collections.singletonList("page-views"))
          .addAssignListener(partitions -> log.info("onPartitionsAssigned {}", partitions))
          .addRevokeListener(partitions -> log.info("onPartitionsRevoked {}", partitions));

      subscribe = KafkaReceiver.create(receiverOptions).receive()
          .subscribe(r -> {
             log.info("Got page view: " + r.value());
             r.receiverOffset().acknowledge();
          });
      System.out.println("Started listening");
   }
}

@Slf4j
@RequiredArgsConstructor
@Component
class KafkaPublisher {
   public static final String[] PAGES = {"home", "blog", "teachable"};
   public static final String[] USERS = {"kbeck", "jbrains", "smancuso"};
   public static final Random random = new Random();

   @PostConstruct
   public void startPublisher() {
      Flux<PageView> pagesFlux = Flux.interval(ofSeconds(1))
          .map(i -> new PageView(pickRandom(USERS), pickRandom(PAGES)));

      Map<String, Object> props = new HashMap<>();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
      props.put(ProducerConfig.ACKS_CONFIG, "all");
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      SenderOptions<String, String> senderOptions = SenderOptions.create(props);

      KafkaSender.create(senderOptions)
          .send(pagesFlux.map(pageView -> SenderRecord.create(new ProducerRecord<>("page-views", pageView.getUser(), pageView.getPage()), pageView.toString())))
          .subscribe(
              r -> log.info("Message {} sent successfully {}", r.correlationMetadata(), toString(r.recordMetadata())),
              e -> log.error("Send failed", e));
   }

   private String pickRandom(String[] arr) {
      return arr[random.nextInt(arr.length)];
   }

   public String toString(RecordMetadata r) {
      return "topic: " + r.topic() + ", partition: " + r.partition() + ", offset: " + r.offset();

   }
}

class PageView {
   private final String user;
   private final String page;

   PageView(String user, String page) {
      this.user = user;
      this.page = page;
   }

   public String getPage() {
      return page;
   }

   public String getUser() {
      return user;
   }

   @Override
   public String toString() {
      return user + '-' + page;
   }

   public static PageView fromString(String s) {
      String[] split = s.split("-");
      return new PageView(split[0], split[1]);
   }
}