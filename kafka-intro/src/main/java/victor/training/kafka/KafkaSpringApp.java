package victor.training.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;
import victor.training.kafka.interceptor.ConsumerInterceptor;

import java.util.Collection;
import java.util.Map;

@Slf4j
@SpringBootApplication
//@EnableScheduling
public class KafkaSpringApp {
  public static void main(String[] args) {
    SpringApplication.run(KafkaSpringApp.class, args);
  }

  @Bean
  public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> kafkaContainerCustomizer() {
    return container -> {
      container.setRecordInterceptor(new ConsumerInterceptor());
      container.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          if (!partitions.isEmpty()) log.info("Assigned {}", partitions);
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          if (!partitions.isEmpty()) log.info("Revoked {}", partitions);
        }
      });
    };
  }


  @EventListener(ApplicationStartedEvent.class)
  public void onStartup() {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
  }
}
