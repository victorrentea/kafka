package victor.training.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import victor.training.kafka.interceptor.ConsumerTraceIdInterceptor;

@SpringBootApplication
@EnableScheduling
public class KafkaApp {
  public static void main(String[] args) {
    SpringApplication.run(KafkaApp.class, args);
  }

  @Bean
  public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> kafkaContainerCustomizer() {
    return container -> container.setRecordInterceptor(new ConsumerTraceIdInterceptor());
  }

}
