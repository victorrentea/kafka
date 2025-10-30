package victor.training.kafka.batch_consume;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
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


//   @KafkaListener(topics = BATCH_IN_TOPIC) // Traditional: process one message at a time
  public void consume(ConsumerRecord<String, String> record) {
    Map<String, Product> productsById = fetchManyFromRemote(List.of(record.value()));
    Product enrichedProduct = productsById.get(record.value());
    kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
  }

  @KafkaListener(topics = BATCH_IN_TOPIC, batch = "true")
  public void consume(List<ConsumerRecord<String, String>> records) {
    log.info("Process: {} records : {}", records.size(), records);

    List<String> productIds = records.stream().map(ConsumerRecord::value).toList();
    Map<String, Product> productsById = fetchManyFromRemote(productIds);
    for (var record : records) {
      var productId = record.value();
      Product enrichedProduct = productsById.get(productId);
      kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), enrichedProduct);
    }
//    randomError(); // requires transactional (see kafka-tx module)
  }

  private void randomError() {
    if (Math.random()<.5) {
      log.error("DUMMY ERROR");
      throw new RuntimeException("BUG🐞");
    }
  }

  @SneakyThrows
  private Map<String, Product> fetchManyFromRemote(List<String> productIds) {
    Thread.sleep(100+productIds.size());
    return productIds.stream().collect(toMap(Function.identity(), id ->
        new Product(id, "name-" + id, "desc-" + id)));
  }

}
