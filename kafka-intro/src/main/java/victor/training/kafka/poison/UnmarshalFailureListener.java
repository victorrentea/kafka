package victor.training.kafka.poison;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.intro.Event;

import java.util.Map;

@Slf4j
@RequiredArgsConstructor
//@RestController
// WIP: not done ❌❌❌❌❌❌
public class UnmarshalFailureListener implements ConsumerSeekAware {

  private static final String POISON_TOPIC = "poison";
  private final KafkaTemplate<String, Event> kafkaTemplateOk;
  private final KafkaTemplate<String, Long> kafkaTemplateKo;
  @GetMapping("poison/good") // http://localhost:8080/poison/good
  public void sendPoisonGood() {
    kafkaTemplateOk.send(POISON_TOPIC, "key1", new Event.EventOK("work"));
  }

  @GetMapping("poison/bad") // http://localhost:8080/poison/bad
  public void sendPoisonBad() {
    kafkaTemplateKo.send(POISON_TOPIC, "key1", 46L);
  }

  @KafkaListener(topics = POISON_TOPIC,
      properties = {
          "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer=victor.training.kafka.intro.Event$Deserializer"
      })
  public void consume(Event event) {
    log.info("Consumed event: {}", event);
    //never ack
  }

  @Override
  public void onPartitionsAssigned(Map<TopicPartition, Long> assignments,
                                   ConsumerSeekAware.ConsumerSeekCallback callback) {
    log.info("Seek to start on: {}", assignments);
    callback.seekToBeginning(assignments.keySet());
  }

}
