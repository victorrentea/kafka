package victor.training.kafka.game;

import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.String;
import static org.assertj.core.api.Assertions.assertThat;
import static victor.training.kafka.game.GameTopology.COMPENSATING_PAYOUT_TOPIC;
import static victor.training.kafka.game.GameTopology.GAME_FINISHED_TOPIC;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class GameTopologyTest {
  public static final Duration DURATION = Duration.ofDays(7);
  public static final int CONSECUTIVE_LOSSES = 5;
  public static final int COMPENSATION_AMOUNT = 10;
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, GameFinished> gameFinishedInTopic;
  private TestOutputTopic<String, CompensatingPayout> compensationPayoutOutTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    GameTopology.topology(streamsBuilder, CONSECUTIVE_LOSSES, COMPENSATION_AMOUNT, DURATION);
    Topology topology = streamsBuilder.build();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    gameFinishedInTopic = testDriver.createInputTopic(GAME_FINISHED_TOPIC, String().serializer(), new JsonSerde<>(GameFinished.class).serializer());
    compensationPayoutOutTopic = testDriver.createOutputTopic(COMPENSATING_PAYOUT_TOPIC, String().deserializer(), new JsonSerde<>(CompensatingPayout.class).deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  GameFinished.GameFinishedBuilder lossEvent = GameFinished.builder()
      .userId("user")
      .gameType("game")
      .betAmount(1)
      .payoutAmount(0)
      .timestamp(Instant.now().toEpochMilli());

  @Test
  void tooFew() {
    for (int i = 0; i < CONSECUTIVE_LOSSES - 1; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).build());
    }

    assertThat(compensationPayoutOutTopic.readKeyValuesToList()).isEmpty();
  }

  @Test
  void trigger() {
    for (int i = 0; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).build());
    }

    assertThat(compensationPayoutOutTopic.readKeyValuesToList())
        .containsExactly(new KeyValue<>("user", new CompensatingPayout("user", COMPENSATION_AMOUNT, lossEvent.build().timestamp())));
  }

  @Test
  void nonConsecutiveLosses() {
    gameFinishedInTopic.pipeInput(lossEvent.roundId("round-A").build());
    gameFinishedInTopic.pipeInput(lossEvent.roundId("round-B").payoutAmount(50).build());
    for (int i = 2; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).build());
    }

    assertThat(compensationPayoutOutTopic.readKeyValuesToList()).isEmpty();
  }

  @Test
  void tooSoon() {
    long t0 = Instant.now().toEpochMilli();
    for (int i = 0; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).timestamp(t0).build());
    }
    long lateTs = Instant.now().toEpochMilli() + DURATION.toMillis() - 3000;
    for (int i = 0; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).timestamp(lateTs).build());
    }

    assertThat(compensationPayoutOutTopic.readKeyValuesToList())
        .containsExactly(new KeyValue<>("user", new CompensatingPayout("user", COMPENSATION_AMOUNT, t0)));
  }

  @Test
  void soonAfter() {
    long t0 = Instant.now().toEpochMilli();
    for (int i = 0; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-" + i).timestamp(t0).build());
    }
    long lateTs = t0 + DURATION.toMillis() + 10000;
    for (int i = 0; i < CONSECUTIVE_LOSSES; i++) {
      gameFinishedInTopic.pipeInput(lossEvent.roundId("round-B" + i).timestamp(lateTs).build());
    }

    assertThat(compensationPayoutOutTopic.readKeyValuesToList())
        .containsExactly(
            new KeyValue<>("user", new CompensatingPayout("user", COMPENSATION_AMOUNT, t0)),
            new KeyValue<>("user", new CompensatingPayout("user", COMPENSATION_AMOUNT, lateTs))
        );
  }
  // TODO per game too
}
