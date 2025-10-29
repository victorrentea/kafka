package victor.training.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.transaction.annotation.Transactional;
import victor.training.kafka.BatchEnricherConsumer.Product;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.BatchEnricherConsumer.BATCH_IN_TOPIC;
import static victor.training.kafka.BatchEnricherConsumer.BATCH_OUT_TOPIC;

@Slf4j
//@Transactional
//  @ResetOffsets
@ResetKafkaOffsets({BATCH_IN_TOPIC, BATCH_OUT_TOPIC})
class BatchEnricherConsumerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> longKafkaTemplate;

  private static final BlockingQueue<Product> outQueue = new LinkedBlockingQueue<>();

  @BeforeEach
  void clear() {
    outQueue.clear();
  }

  @AfterEach
  void after() {
    outQueue.clear();
  }

  @Test
  void functional_enrichment() throws Exception {
    longKafkaTemplate.send(BATCH_IN_TOPIC, "k1", "1");
    longKafkaTemplate.send(BATCH_IN_TOPIC, "k2", "2");
    longKafkaTemplate.send(BATCH_IN_TOPIC, "k3", "3");
    Thread.sleep(5000);
//
    List<Product> received = new ArrayList<>();
    received.add(outQueue.poll(10, SECONDS));
    received.add(outQueue.poll(2, SECONDS));
    received.add(outQueue.poll(2, SECONDS));
//
    assertThat(received.stream().map(Product::name))
        .containsExactlyInAnyOrder("name-1","name-2","name-3");
  }

//  @Test
  void performance_batch_under_threshold() throws Exception {
    int n = 100;
    long start = System.currentTimeMillis();
    for (int i = 1; i <= n; i++) {
      longKafkaTemplate.send(BATCH_IN_TOPIC, "k" + i, ""+i);
    }

    int receivedCount = 0;
    while (receivedCount < n) {
      Product p = outQueue.poll(20, SECONDS);
      assertThat(p).isNotNull();
      receivedCount++;
    }
    long elapsedMs = System.currentTimeMillis() - start;
    log.info("Elapsed for {} events: {} ms", n, elapsedMs);

    // With batching, fetchMany sleeps ~100 + n ms; allow generous overhead for Kafka and CI
    // This threshold ensures we didn't do n individual fetches which would take ~n*100ms (~10s for n=100)
    assertThat(elapsedMs).isLessThan(5000);
  }

  @TestConfiguration
  public static class ListenerCfg {
    @KafkaListener(topics = BATCH_OUT_TOPIC,
        groupId = "batch-enricher-func-perf-test",
        properties = {
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG + "=latest",
            ConsumerConfig.ISOLATION_LEVEL_CONFIG + "=read_committed",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG + "=org.apache.kafka.common.serialization.StringDeserializer",
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG + "=org.springframework.kafka.support.serializer.JsonDeserializer"
        })
    public void listen(Product product) {
      outQueue.add(product);
    }
  }
}