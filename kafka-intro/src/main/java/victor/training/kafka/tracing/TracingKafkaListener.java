package victor.training.kafka.tracing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import victor.training.kafka.intro.Event;

@Slf4j
@Component
public class TracingKafkaListener {
  private final RestClient restClient = RestClient.create();

  @KafkaListener(topics = TracingInController.TOPIC, groupId = "tracing-app")
  public void onMessage(Event event) {
    log.info("STEP 3 — Kafka message consumed: {}", event);
    String response = restClient.get()
        .uri("http://localhost:8080/tracing/downstream")
        .retrieve()
        .body(String.class);
    log.info("STEP 4 — Downstream HTTP call returned: {}", response);
  }
}
