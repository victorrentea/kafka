package victor.training.kafka.time;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
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
public class TimeTopologyTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> input;
  private TestOutputTopic<String, String> tumbling;
  public static final Instant T0 = Instant.now();

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    TimeTopology.topology(streamsBuilder);
    Topology topology = streamsBuilder.build();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    input = testDriver.createInputTopic("time-input", String().serializer(), String().serializer());
    tumbling = testDriver.createOutputTopic("time-tumbling", String().deserializer(), String().deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void explore() {
    for (int i = 0; i < 20; i++) {
      input.pipeInput("k", "A",T0.plusSeconds(i).toEpochMilli());
    }

    assertThat(tumbling.readValuesToList())
        .containsExactly("AAAA", "AAAAA", "AAAAA", "AAAAA");

  }
}
