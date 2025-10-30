package victor.training.kafka.test.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Component
public class TestedListener {
  public static final String IN_TOPIC = "tested-in";
  private final AService aService;

  @Bean
  public NewTopic testInTopic() {
    return TopicBuilder.name(IN_TOPIC)
        .partitions(2)
        .build();
  }

  @KafkaListener(topics = IN_TOPIC)
  public void consume(String message) {
    log.info("Consuming " + message);
    aService.logic(message);
  }
}

@Service
@RequiredArgsConstructor
class AService{
  private final ARepo aRepo;

  public void logic(String message) {
    aRepo.save(message);
    // more logic
  }
}
@Repository
class ARepo {
  public void save(String message) {
    // irrelevant
  }
}