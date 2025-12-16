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
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.backoff.FixedBackOff;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class RaceListener {
  private final RaceRepo raceRepo;
  public static final String RACE_TOPIC = "race-topic";

  @Bean
  public NewTopic raceTopic() {
    return TopicBuilder.name(RACE_TOPIC)
        .partitions(3)
        .build();
  }

  public record Message(String id, int seq) {}

  @KafkaListener(topics = RACE_TOPIC, concurrency = "3")
  @Transactional // SQL
  public void consume(Message message) throws InterruptedException {
    RaceEntity entity = raceRepo.findById(message.id()).orElseThrow();
    entity.total(entity.total() + 1);
    Thread.sleep(3); // ~ network call; larger => higher race chances
  }

  @Configuration
  static class Config {
    @Bean // TODO use me in case of Giveup retry on recurrent Optimistic Locking errors
    public ConcurrentKafkaListenerContainerFactory<String, String>
    lotsOfRetries(ConsumerFactory<String, String> consumerFactory) {
      var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
      factory.setConsumerFactory(consumerFactory);
      var errorHandler = new DefaultErrorHandler(new FixedBackOff(50L, 100));
      errorHandler.addRetryableExceptions(ObjectOptimisticLockingFailureException.class);
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

//  @Version
//  Long version;
}
