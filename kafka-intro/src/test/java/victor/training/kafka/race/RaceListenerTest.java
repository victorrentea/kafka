package victor.training.kafka.race;

import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.race.RaceListener.Message;
import victor.training.kafka.testutil.ResetKafkaOffsets;

import javax.management.Query;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static victor.training.kafka.race.RaceListener.TOPIC;

@Slf4j
@ResetKafkaOffsets(TOPIC)
public class RaceListenerTest extends KafkaTest {
  public static final int N = 1000;
  public static final String ID = "id";
  @Autowired
  KafkaTemplate<String, Message> kafkaTemplate;
  @Autowired
  RaceRepo raceRepo;
  @MockitoSpyBean
  RaceListener listener;

  @Test
  @Disabled("TODO")
  void ok() throws InterruptedException {
    raceRepo.save(new RaceEntity().id(ID).total(0));
    for (int i = 0; i < N; i++) {
      kafkaTemplate.send(TOPIC,  new Message(ID, i));
      // Fix#1: partition key
      // Fix#2: JPA optimistic locking (WARNING: message is discarded after 10 optimistic locking errors)
    }

    await().atMost(ofSeconds(15)).untilAsserted(()->
      assertThat(raceRepo.findById(ID).orElseThrow().total()).isEqualTo(N)
    );
  }

}
