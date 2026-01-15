package victor.training.kafka.transactions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@RequiredArgsConstructor
@Service
public class TransactionalProcessor {
  public static final String IN_TOPIC = "transactional-in";
  public static final String OUT_TOPIC_A = "transactional-out-a";
  public static final String OUT_TOPIC_B = "transactional-out-b";

  private final KafkaTemplate<?, String> kafkaTemplate;

  @KafkaListener(topics = IN_TOPIC, containerFactory = "transactedKafkaListenerContainerFactory")
    @Transactional(transactionManager = "kafkaTransactionManager")
  public void atomicConsumeAndSendN(String message) {
    log.info("START consuming: {}", message);
    if(message.equals("fail-at-step-1")) throw new IllegalArgumentException("1");
    kafkaTemplate.send(OUT_TOPIC_A, "A1"); // A
    if(message.equals("fail-at-step-2")) throw new IllegalArgumentException("2");
    kafkaTemplate.send(OUT_TOPIC_A, "A2"); // 2 x A
    if(message.equals("fail-at-step-3")) throw new IllegalArgumentException("3");
    kafkaTemplate.send(OUT_TOPIC_B, "B"); // 2 x A + B
    if(message.equals("fail-at-step-4")) throw new IllegalArgumentException("4");
  }

  @Configuration
  public static class Config {
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String>
    transactedKafkaListenerContainerFactory(
        ConsumerFactory<String, String> consumerFactory,
        KafkaTransactionManager<Object, Object> kafkaTransactionManager) {

      var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
      factory.setConsumerFactory(consumerFactory);
      DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(500L, 1));
      // TODO consider: errorHandler#addNotRetryableExceptions
      factory.setCommonErrorHandler(errorHandler);
      factory.getContainerProperties().setKafkaAwareTransactionManager(kafkaTransactionManager);
      return factory;
    }
  }
}
