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
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.sim.OutOfOrderListener.SIM_TOPIC;
import static victor.training.kafka.sim.SimEvent.CreditAdded;
import static victor.training.kafka.sim.SimEvent.OfferActivated;

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
    kafkaTemplate.send(SIM_TOPIC, UUID.randomUUID().toString(), new CreditAdded(simId, 10));
    kafkaTemplate.send(SIM_TOPIC, UUID.randomUUID().toString(), new OfferActivated(simId, "National10", 10));

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
