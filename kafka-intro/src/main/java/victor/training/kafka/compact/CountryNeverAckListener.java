package victor.training.kafka.compact;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL;
import static victor.training.kafka.compact.CompactTopicConfig.COUNTRY_TOPIC;

@Slf4j
@Component
public class CountryNeverAckListener {
  @KafkaListener(topics = COUNTRY_TOPIC, groupId = "never-ack",
      properties = {
          // at every app restart you see all events in COUNTRY_TOPIC
          "auto.offset.reset=earliest",
          // + a retention time = infinite since extra-rarely added
          "enable.auto.commit=false",
          "key.deserializer=org.apache.kafka.common.serialization.StringDeserializer",
          "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer"
      },
      containerFactory = "manualAckContainerFactory")
  public void consumeNeverAck(ConsumerRecord<String, String> countryRecord, Acknowledgment ack) {
    log.info("B) Got country: " + countryRecord.value());
    // never commited: ack.acknowledge();
  }

  @Configuration
  public static class Config {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    manualAckContainerFactory(ConsumerFactory<String, String> consumerFactory) {
      var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
      factory.setConsumerFactory(consumerFactory);
      factory.getContainerProperties().setAckMode(MANUAL);
      return factory;
    }
  }
}
