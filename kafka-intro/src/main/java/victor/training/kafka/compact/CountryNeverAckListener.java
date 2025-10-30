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
  // B) Never ack messages
  @KafkaListener(topics = COUNTRY_TOPIC, groupId = "never-ack",
      properties = {
          "auto.offset.reset=earliest",
          "enable.auto.commit=false"
      },
      containerFactory = "manualAckKafkaListenerContainerFactory")
  public void consumeNeverAck(ConsumerRecord<String, String> countryRecord, Acknowledgment ack) {
    log.info("B) Got country: " + countryRecord.value());
    // never ACK
  }

  @Configuration
  public static class Config {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    manualAckKafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory) {
      var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
      factory.setConsumerFactory(consumerFactory);
      factory.getContainerProperties().setAckMode(MANUAL);
      return factory;
    }
  }
}
