package victor.training.kafka.ooo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.ooo.OutOfOrderDelayRetryListener.TOPIC;

@Slf4j
public class OutOfOrderDelayRetryListenerTest extends KafkaTest {
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private OutOfOrderDelayRetryListener outOfOrderListener;

  @BeforeEach
  final void before() {
      outOfOrderListener.pairs = 0;
      outOfOrderListener.pendingOpen = 0;

      // Curata mesajele ramase dupa testele precedente:
      // VARIANTA (a): consuma si ignora mesajele din topicele de retry/DLT pentru X secunde
//      KafkaCleanup.drainRetryAndDltTopics("localhost:9092", TOPIC, Duration.ofSeconds(1));

      // VARIANTA (b): seteaza offsetul grupului de consumatori la ultimul pentru topicul de baza si topicele derivate (-retry/-dlt)
      // Nota: groupId-ul implicit este cel din application.yaml: spring.kafka.consumer.group-id=app
      // Daca vrei sa folosesti varianta b, decomenteaza linia de mai jos si eventual comenteaza varianta (a) de mai sus
      // KafkaCleanup.advanceGroupOffsetsToEndForBaseTopic("localhost:9092", "app", TOPIC);
  }
  @Test
  void ok() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "("); // transmission_started
    kafkaTemplate.send(TOPIC, ")"); // transmission_ended

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(1);
  }
  @Test
  void ok2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, ")");

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(2);
  }
  @Test
  void scrambled() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")"); // OfferActivated(Nat5E)
    kafkaTemplate.send(TOPIC, "("); // PrePaySimReloaded(5 EUR)

    Thread.sleep(2000);
    assertThat(outOfOrderListener.pairs).isEqualTo(1);
  }
  @Test
  void scrambled2() throws InterruptedException {
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, ")");
    kafkaTemplate.send(TOPIC, "(");
    kafkaTemplate.send(TOPIC, "(");

    Thread.sleep(5000);
    assertThat(outOfOrderListener.pairs).isEqualTo(2);
  }

}
