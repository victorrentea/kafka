package victor.training.kafka.compact;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

import static victor.training.kafka.compact.CompactTopicConfig.COUNTRY_TOPIC;

@Component
@Slf4j
@RequiredArgsConstructor
public class CountrySeekToStartListener implements ConsumerSeekAware {
  @Override
  public void onPartitionsAssigned(
      Map<TopicPartition, Long> assignments,
      ConsumerSeekCallback callback) {
    log.info("Seek to start on: {}", assignments);
    callback.seekToBeginning(assignments.keySet());
  }

  @KafkaListener(topics = COUNTRY_TOPIC,
      properties = {
          "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
      })
  public void consumeSeeking(ConsumerRecord<String, String> countryRecord) {
    log.info("A) Got country: " + countryRecord.value());
  }


}
