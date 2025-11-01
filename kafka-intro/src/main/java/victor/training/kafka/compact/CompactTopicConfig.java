package victor.training.kafka.compact;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.runAsync;

@Configuration
@RequiredArgsConstructor
public class CompactTopicConfig {
  public static final String COUNTRY_TOPIC = "countries";
  private final KafkaTemplate<String, String> kafkaTemplate;

  @Bean
  public NewTopic countriesTopic() {
    System.out.println("Creating");
    return TopicBuilder.name(COUNTRY_TOPIC)
        .partitions(2)
        .replicas(2)
        .compact() // Kafka can remove previous records for the same key **only** when compacting

        // the settings below aim to cause earlier compaction (!only for demo purposes)
        .config("segment.ms", "1000")
        .config("min.cleanable.dirty.ratio","0.01")
        .config("min.compaction.lag.ms","1")
        // To apply on existing topics: ./kafka-configs.sh --alter --topic ...

        //.config("retention.ms","5000") // TODO experiment auto-delete record time (default: 7 days)
        .build();
  }

  @EventListener(ApplicationStartedEvent.class)
  public void initial() {
    kafkaTemplate.send(COUNTRY_TOPIC, "RO", "Kingdom of Romania");
    kafkaTemplate.send(COUNTRY_TOPIC, "RO", "People’s Republic of Romania");
    kafkaTemplate.send(COUNTRY_TOPIC, "RO", "Socialist Republic of Romania");
    kafkaTemplate.send(COUNTRY_TOPIC, "RO", "Romania");

    kafkaTemplate.send(COUNTRY_TOPIC, "CS", "Czechoslovakia");
    kafkaTemplate.send(COUNTRY_TOPIC, "CS", null);// tombstone in 1993
    kafkaTemplate.send(COUNTRY_TOPIC, "CZ", "Czech Republic");
    kafkaTemplate.send(COUNTRY_TOPIC, "SK", "Slovakia");

    kafkaTemplate.send(COUNTRY_TOPIC, "FR", "France");
    runAsync(() -> kafkaTemplate.send(COUNTRY_TOPIC, "FR", "France"),
        delayedExecutor(2, TimeUnit.SECONDS));
  }
}
