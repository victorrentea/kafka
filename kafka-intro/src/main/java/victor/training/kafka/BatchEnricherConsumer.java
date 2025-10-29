package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;

@Slf4j
@RequiredArgsConstructor
@Service
public class BatchEnricherConsumer {
  public static final String BATCH_IN_TOPIC = "batch-in-topic";
  public static final String BATCH_OUT_TOPIC = "batch-out-topic";

  private final KafkaTemplate<String, Product> kafkaTemplate;

  public record Product(String id, String name, String description){}

  @KafkaListener(topics = BATCH_IN_TOPIC, batch = "true")
//  @Transactional(transactionManager = "kafkaTransactionManager")
  public void consumeBatch(List<ConsumerRecord<String, String>> records) {
    log.info("Process: {} records : {}", records.size(), records);
    List<String> productIds = records.stream().map(ConsumerRecord::value).toList();
    Map<String, Product> productsById = fetchMany(productIds);
    for (var record : records) {
      var productId = record.value();
      Product enrichedProduct = productsById.get(productId);
//      if (random.nextDouble() < .3) throw new IllegalArgumentException("BOOM");
      kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
    }
  }

  // --- Traditional non-batch implementation (for educational purposes) ---
  // Processes one message at a time  would call fetchMany for every single record, leading to significantly worse performance.
//   @KafkaListener(topics = BATCH_IN_TOPIC)
   public void consumeOne(ConsumerRecord<String, String> record) {
     Map<String, Product> productsById = fetchMany(List.of(record.value()));
     Product enrichedProduct = productsById.get(record.value());
     kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
   }

  @SneakyThrows
  private Map<String, Product> fetchMany(List<String> productIds) {
    Thread.sleep(100+productIds.size());
    return productIds.stream().collect(toMap(Function.identity(), this::dummyProduct));
  }

  private Product dummyProduct(String id) {
    return new Product(id, "name-" + id, "desc-" + id);
  }
}
