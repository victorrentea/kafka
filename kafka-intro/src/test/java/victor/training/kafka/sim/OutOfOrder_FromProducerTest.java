package victor.training.kafka.sim;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
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
import static victor.training.kafka.sim.SimEvent.ActivateOffer;
import static victor.training.kafka.sim.SimEvent.AddCredit;

@SpringBootTest
//@EmbeddedKafka // or via Kafka from docker-compose.yaml
public class OutOfOrder_FromProducerTest {
  @Autowired
  private KafkaTemplate<String, SimEvent> kafkaTemplate;
  @Autowired
  private SimRepo simRepo;

  @Test
  void sentInIncorrectOrder() throws InterruptedException {
    var simId = simRepo.save(new Sim()).id();
    Thread.sleep(50);// WTF?!
    var messageKey = simId+"";
    var addCreditTs = System.currentTimeMillis() - 100;
    var activateOfferTs = System.currentTimeMillis();

    kafkaTemplate.send(SIM_TOPIC, 1, activateOfferTs, messageKey, new ActivateOffer(simId, "National10", 10));
    kafkaTemplate.send(SIM_TOPIC, 1, addCreditTs, messageKey, new AddCredit(simId, 10, UUID.randomUUID().toString()));

    Awaitility.await()
        .pollInterval(ofMillis(500))
        .timeout(ofSeconds(3))
        .untilAsserted(() ->
            assertThat(simRepo.findById(simId).orElseThrow())
                .returns("National10", Sim::activeOfferId));
  }

  @TestConfiguration
  static class NoRetryConfig {
    @Bean
    public DefaultErrorHandler errorHandler() {
      // disable Spring Kafka's default retry x 10
      return new DefaultErrorHandler(new FixedBackOff(10, 2)); // didn't help
    }
  }

}
