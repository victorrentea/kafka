package victor.training.kafka.race;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.backoff.FixedBackOff;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class RaceListener {
  private final RaceRepo raceRepo;
  public static final String TOPIC = "race-topic";

  @Bean
  public NewTopic raceTopic() {
    return TopicBuilder.name(TOPIC)
        .partitions(3)
        .build();
  }

  record Message(String id, int seq) {}

  @KafkaListener(topics = TOPIC, concurrency = "3",containerFactory = "multeRetryuri")
  @Transactional // DB
  public void consume(Message message) throws InterruptedException {
    RaceEntity entity = raceRepo.findById(message.id()).orElseThrow();
    entity.total(entity.total() + 1);
    Thread.sleep(20); // ~ network call; larger => higher race chances
  }

  @Configuration
  static class Config {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    multeRetryuri(ConsumerFactory<String, String> consumerFactory) {
      var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
      factory.setConsumerFactory(consumerFactory);
      DefaultErrorHandler errorHandler = new DefaultErrorHandler(
          new FixedBackOff(50L, 100));
      factory.setCommonErrorHandler(errorHandler);
      return factory;
    }
  }
}


interface RaceRepo extends JpaRepository<RaceEntity, String> {
}

@Entity
@Data
class RaceEntity {
  @Id
  String id = UUID.randomUUID().toString();

  Integer total = 0;

  @Version
  Long version;
}
