package victor.training.kafka.race;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.IntegrationTest;
import victor.training.kafka.race.RaceListener.Message;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import java.util.UUID;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static victor.training.kafka.race.RaceListener.RACE_TOPIC;

@Slf4j
@ResetKafkaOffsets(RACE_TOPIC)
public class RaceListenerTest extends IntegrationTest {
  public static final String CLIENT_ID = UUID.randomUUID().toString();
  @Autowired
  KafkaTemplate<String, Message> kafkaTemplate;
  @Autowired
  RaceRepo raceRepo;

  @Test
  void ok() throws InterruptedException {
    raceRepo.save(new RaceEntity().id(CLIENT_ID).total(0));
    final int N = 1000;
    for (int i = 0; i < N; i++) {
      kafkaTemplate.send(RACE_TOPIC, new Message(CLIENT_ID, i));
      // Fix#1: partition key
      // Fix#2: JPA optimistic locking
      //   >WARNING: message can get lost after 10 optimistic locking errors
    }

    await().atMost(ofSeconds(150)).untilAsserted(() -> // default: every 100ms
      assertThat(raceRepo.findById(CLIENT_ID).orElseThrow().total()).isEqualTo(N)
    );
  }

}
