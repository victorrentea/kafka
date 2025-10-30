package victor.training.kafka.sim;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;
import victor.training.kafka.IntegrationTest;

import java.util.UUID;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.sim.SimEvent.CreditAdded;
import static victor.training.kafka.sim.SimEvent.OfferActivated;

public class SimEventListenerTest  extends IntegrationTest {
  @Autowired
  protected KafkaTemplate<String, SimEvent> kafkaTemplate;
  @Autowired
  private SimRepo simRepo;

  @RepeatedTest(5)
  void messagesShouldBeConsumedInTheSameOrder() {
    var simId = simRepo.save(new Sim()).id();
    kafkaTemplate.send(SimEventListener.SIM_TOPIC, UUID.randomUUID().toString(), new CreditAdded(simId, 10));
    kafkaTemplate.send(SimEventListener.SIM_TOPIC, UUID.randomUUID().toString(), new OfferActivated(simId, "National10", 10));

    Awaitility.await()
        .pollInterval(ofMillis(500))
        .timeout(ofSeconds(1))
        .untilAsserted(() ->
            assertThat(simRepo.findById(simId).orElseThrow())
                .returns("National10", Sim::activeOfferId));
  }

  @Test
  void messagesShouldBeConsumedInTheOrderOfTheirTimestamp() {
    var simId = simRepo.save(new Sim()).id();
    long ts2 = System.currentTimeMillis();
    long ts1 = ts2-100; // observed earlier
    kafkaTemplate.send(SimEventListener.SIM_TOPIC,1, ts2, simId + "", new OfferActivated(simId, "National10", 10));
    kafkaTemplate.send(SimEventListener.SIM_TOPIC,1, ts1, simId + "", new CreditAdded(simId, 10));

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
      // override spring-kafka's default = 10 attempts x 0.5 sec interval
      return new DefaultErrorHandler(new FixedBackOff(0, 0));
    }
  }

}
