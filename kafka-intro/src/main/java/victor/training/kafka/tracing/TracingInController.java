package victor.training.kafka.tracing;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.intro.Event;

@Slf4j
@RestController
@RequestMapping("/tracing")
@RequiredArgsConstructor
public class TracingInController {
  public static final String TOPIC = "tracing-topic";

  private final KafkaTemplate<String, Event> kafkaTemplate;

  @GetMapping("/start")
  public String start() {
    log.info("STEP 1 — HTTP IN received: about to publish to Kafka");
    kafkaTemplate.send(TOPIC, new Event.EventOK("trace-me"));
    log.info("STEP 2 — Kafka message sent");
    return "started — check the logs for trace_id propagation";
  }

  @Bean
  public NewTopic tracingTopic() {
    return TopicBuilder.name(TOPIC).partitions(1).build();
  }
}
