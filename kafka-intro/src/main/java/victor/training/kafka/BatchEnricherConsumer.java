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

      // Simulate a random failure inside the transaction (at most once) to prove transactional semantics.
//      maybeRandomFail();

      kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
    }
  }

  // --- Alternative non-batch implementation (for educational purposes) ---
  // This version processes one message at a time and would call fetchMany for every single record,
  // leading to significantly worse performance. Left here intentionally commented out.
  //
  // @KafkaListener(topics = BATCH_IN_TOPIC, batch = "false",
  //     properties = {
  //         ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringDeserializer",
  //         ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.LongDeserializer"
  //     })
  // @Transactional(transactionManager = "kafkaTransactionManager")
  // public void consumeOne(ConsumerRecord<String, Long> record) {
  //   Map<Long, Product> productsById = fetchMany(List.of(record.value()));
  //   Product enrichedProduct = productsById.get(record.value());
  //   kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
  // }

  private final Random random = new Random();
  private final AtomicBoolean alreadyFailedOnce = new AtomicBoolean(false);
  // Probability of failing a send within a batch once; configurable via Spring property
  // batchEnricher.randomFailureProb (default 0.05). Tests can override to 0.0/1.0 as needed.

  private void maybeRandomFail() {
    if (!alreadyFailedOnce.get() && random.nextDouble() < .5) {
      alreadyFailedOnce.set(true);
      throw new RuntimeException("Random failure to test transactional batch retry");
    }
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
