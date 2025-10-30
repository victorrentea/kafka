package victor.training.kafka.compact;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class CompactTopicConfig {
  public static final String COUNTRY_TOPIC = "countries";

  @Bean
  public NewTopic countriesTopic() {
    return TopicBuilder.name(COUNTRY_TOPIC)
        .partitions(1)
        .replicas(2)
        .compact() // are voie sa stearga recordurile precedente pentru aceeasi cheie, cand Kafka vrea sa faca curat
        // trigger an eager compactation (only for demo purposes)
        .config("segment.ms", "1000")
        .config("min.cleanable.dirty.ratio","0.01")
        .config("min.compaction.lag.ms","0")

//        .config("retention.ms","10000") // auto-delete record after this. default: 7 days
        .build();
  }
  //  ./kafka-configs.sh --bootstrap-server localhost:9092 --alter --topic countries \
//      --add-config segment.ms=10000,min.cleanable.dirty.ratio=0.01
  // ± restart la broker

//  @PostConstruct
//  public void initial() {
//    kafkaTemplate.send(COUNTRY_TOPIC, "RO", "Romania");
//  }
}
