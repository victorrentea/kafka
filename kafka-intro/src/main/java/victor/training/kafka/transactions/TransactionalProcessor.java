package victor.training.kafka.transactions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
public class TransactionalProcessor {
  public static final String IN = "transactional-in";
  public static final String OUT_A = "transactional-out-a";
  public static final String OUT_B = "transactional-out-b";

  private final KafkaTemplate<?, String> kafkaTemplate;

  @KafkaListener(topics = IN, containerFactory = "noRetriesKafkaListenerContainerFactory")
  @Transactional(transactionManager = "kafkaTransactionManager")
  public void consume(String message) {
    log.info("::START");
    if(message.equals("1")) throw new IllegalArgumentException("1");

    kafkaTemplate.send(OUT_A, "A1");

    if(message.equals("2")) throw new IllegalArgumentException("2");

    kafkaTemplate.send(OUT_A, "A2");

    if(message.equals("3")) throw new IllegalArgumentException("3");

    kafkaTemplate.send(OUT_B, "B");

    if(message.equals("4")) throw new IllegalArgumentException("4");
    log.info("::END");
  }
}
