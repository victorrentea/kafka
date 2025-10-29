package victor.training.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;
import victor.training.kafka.interceptor.ConsumerInterceptor;

@Slf4j
@SpringBootApplication
//@EnableScheduling
public class KafkaSpringApp {
  public static void main(String[] args) {
    SpringApplication.run(KafkaSpringApp.class, args);
  }

  @Bean
  public ContainerCustomizer<Object, Object, ConcurrentMessageListenerContainer<Object, Object>> kafkaContainerCustomizer() {
    return container -> container.setRecordInterceptor(new ConsumerInterceptor());
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String>
    noRetriesKafkaListenerContainerFactory(
      ConsumerFactory<String, String> consumerFactory) {

    var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
    factory.setConsumerFactory(consumerFactory);
    factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(500L, 1)));
    return factory;
  }

  @EventListener(ApplicationStartedEvent.class)
  public void onStartup() {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
  }
}
