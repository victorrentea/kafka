package victor.training.kafka.race;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.aspectj.runtime.CFlow;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

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

  record Message(String id,int seq){}

  @KafkaListener(topics = TOPIC, concurrency = "3")
  @Transactional // DB + JPA
  public void consume(Message message) throws InterruptedException {
    RaceEntity entity = raceRepo.findById(message.id()).orElseThrow();
    entity.total(entity.total()+1);
    Thread.sleep(5); // increase race chances
  }
}
