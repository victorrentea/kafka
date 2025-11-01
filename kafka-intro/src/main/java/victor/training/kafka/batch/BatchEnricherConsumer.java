package victor.training.kafka.batch;

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
  // Goal: {key:X,value:productId} -> {key:X, value: Product}

//   @KafkaListener(topics = BATCH_IN_TOPIC) // Traditional: process one message at a time = slow
  public void consume(ConsumerRecord<String, String> record) {
    Product product = fetchOneFromRemote(record.value());
    kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), product);
  }

  @KafkaListener(topics = BATCH_IN_TOPIC, batch = "true")
  public void consume(List<ConsumerRecord<String, String>> records) {
    log.info("Received {} records : {}", records.size(), records);

    List<String> productIds = records.stream().map(ConsumerRecord::value).toList();
    Map<String, Product> productsById = fetchManyFromRemote(productIds);

    for (var record : records) {
      var productId = record.value();
      Product product = productsById.get(productId);
      kafkaTemplate.send(BATCH_OUT_TOPIC, record.key(), product);
    }
//    if (Math.random() < .1) throw new RuntimeException("BUG🐞"); // requires @Transactional
  }

  //region support code
  @SneakyThrows
  private Product fetchOneFromRemote(String productId) {
    return fetchManyFromRemote(List.of(productId)).values().iterator().next();
  }
  @SneakyThrows
  private Map<String, Product> fetchManyFromRemote(List<String> productIds) {
    Thread.sleep(100+productIds.size());
    return productIds.stream().collect(toMap(Function.identity(), id ->
        new Product(id, "name-" + id, "desc-" + id)));
  }
  //endregion

}
