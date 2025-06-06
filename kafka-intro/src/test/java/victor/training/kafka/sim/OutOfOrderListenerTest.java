package victor.training.kafka.sim;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.sim.OutOfOrderListener.SIM_TOPIC;
import static victor.training.kafka.sim.SimEvent.AddCredit;
import static victor.training.kafka.sim.SimEvent.ActivateOffer;

@SpringBootTest
//@EmbeddedKafka // or via Kafka from docker-compose.yaml
public class OutOfOrderListenerTest {
  @Autowired
  private KafkaTemplate<String, SimEvent> kafkaTemplate;
  @Autowired
  private SimRepo simRepo;

  @RepeatedTest(5)
  void explore() {
    var simId = simRepo.save(new Sim()).id();

    // temporal coupling of the processing
    var messageKey = simId+""; // groupId in Artemis
    // messages with the same key will be processed sequentially by 1 consumer thread only, in the order they were sent
    // partition to which a kafka message will be sent is hash(key) % partition_count
    kafkaTemplate.send(SIM_TOPIC, randomUUID().toString(), new AddCredit(simId, 10));
    kafkaTemplate.send(SIM_TOPIC, randomUUID().toString(), new ActivateOffer(simId, "National10", 10));

    Awaitility.await()
        .pollInterval(ofMillis(500))
        .timeout(ofSeconds(1))
        .untilAsserted(() ->
            assertThat(simRepo.findById(simId).orElseThrow())
                .returns("National10", Sim::activeOfferId));
  }

  @TestConfiguration
  static class NoRetryConfig {
    @Bean
    public DefaultErrorHandler errorHandler() {
      // disable Spring Kafka's default retry x 10
      return new DefaultErrorHandler(new FixedBackOff(0, 0));
    }
  }

}
