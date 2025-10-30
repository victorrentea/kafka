package victor.training.kafka.batch_consume;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.batch_consume.BatchEnricherConsumer.Product;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.batch_consume.BatchEnricherConsumer.BATCH_IN_TOPIC;
import static victor.training.kafka.batch_consume.BatchEnricherConsumer.BATCH_OUT_TOPIC;

@Slf4j
@ResetKafkaOffsets({BATCH_IN_TOPIC, BATCH_OUT_TOPIC})
class BatchEnricherConsumerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  private static final BlockingQueue<Product> outQueue = new LinkedBlockingQueue<>();

  @BeforeEach
  @AfterEach
  void clear() {
    outQueue.clear();
  }

  @Test
  void functional() throws Exception {
    kafkaTemplate.send(BATCH_IN_TOPIC, "k1", "1");
    kafkaTemplate.send(BATCH_IN_TOPIC, "k2", "2");
    kafkaTemplate.send(BATCH_IN_TOPIC, "k3", "3");

    Thread.sleep(3000);

    assertThat(outQueue.stream().map(Product::name))
        .containsExactlyInAnyOrder("name-1", "name-2", "name-3");
  }

  @Test
  void throughput() throws Exception {
    int n = 100;
    for (int i = 1; i <= n; i++) {
      kafkaTemplate.send(BATCH_IN_TOPIC, "k" + i, "" + i);
    }

    Thread.sleep(4000);

    assertThat(outQueue).describedAs("Number of published messages").hasSize(n);
  }

  @TestConfiguration
  public static class ListenerCfg {
    @KafkaListener(topics = BATCH_OUT_TOPIC, groupId = "batch-enricher-func-perf-test", properties = "auto.offset.reset=latest")
    public void listen(Product product) {
      outQueue.add(product);
    }
  }
}