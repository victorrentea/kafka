package victor.training.kafka.test.sender;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Component
public class TestedSender {
  public static final String OUT_TOPIC = "tested-out";
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final BService bService;

  @Bean
  public NewTopic testRaceTopic() {
    return TopicBuilder.name(OUT_TOPIC)
        .partitions(2)
        .build();
  }

  public void send(String message) {
    bService.logic(message);
    kafkaTemplate.send(OUT_TOPIC, message);
  }
}

@Slf4j
@RequiredArgsConstructor
@Service
class BService {
  public void logic(String message) {
    // irrelevant
  }
}