package victor.training.kafka.metrics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import victor.training.kafka.KafkaUtils;
import victor.training.kafka.words.WordsTopology;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.assertj.core.api.Assertions.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MetricsTopologyTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    KafkaUtils.createTopic("page-views");
    Topology topology = MetricsTopology.topology();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    inputTopic = testDriver.createInputTopic("page-views", String().serializer(), String().serializer());
    outputTopic = testDriver.createOutputTopic("page-views-counter", String().deserializer(), Long().deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void p1_zero() {
    assertThat(outputTopic.readKeyValuesToList()).isEmpty();
  }
  @Test
  void p2_one() {
    inputTopic.pipeInput("user","home");
    testDriver.advanceWallClockTime(Duration.ofSeconds(2));
    assertThat(outputTopic.readKeyValuesToMap()).isEqualTo(Map.of("home", 1L));
  }
  @Test
  void p3_notYet() {
    inputTopic.pipeInput("user","home");
    assertThat(outputTopic.readKeyValuesToMap()).isEmpty();
  }
  @Test
  void p4_twoClose() {
    inputTopic.pipeInput("user","home");
    inputTopic.pipeInput("user","home");
    testDriver.advanceWallClockTime(Duration.ofSeconds(2));
    assertThat(outputTopic.readKeyValuesToMap()).isEqualTo(Map.of("home", 2L));
  }
}
