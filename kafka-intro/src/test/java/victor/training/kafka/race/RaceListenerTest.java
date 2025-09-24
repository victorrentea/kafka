package victor.training.kafka.race;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.KafkaTest;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.race.RaceListener.TOPIC;

@Slf4j
public class RaceListenerTest extends KafkaTest {
  public static final int N = 1000;
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  RaceRepo raceRepo;

  @Test
  @Disabled("TODO")
  void ok() throws InterruptedException {
    raceRepo.save(new RaceEntity().id("a"));
    for (int i = 0; i < N; i++) {
      kafkaTemplate.send(TOPIC, "a");
      // Fix#1: partition key
      // Fix#2: optimistic locking
    }

    Thread.sleep(N*7+1000);

    assertThat(raceRepo.findById("a").orElseThrow().total()).isEqualTo(N);
  }

}
