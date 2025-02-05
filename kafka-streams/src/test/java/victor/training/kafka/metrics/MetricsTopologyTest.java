package victor.training.kafka.metrics;

import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.assertj.core.api.Assertions.*;

@SuppressWarnings("resource")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class MetricsTopologyTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> viewsInTopic;
  private TestOutputTopic<String, Long> viewsCountOutTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    Topology topology = MetricsTopology.topology();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    viewsInTopic = testDriver.createInputTopic("page-views", String().serializer(), String().serializer());
    viewsCountOutTopic = testDriver.createOutputTopic("page-views-count", String().deserializer(), Long().deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void p1_zero() {
    assertThat(viewsCountOutTopic.readKeyValuesToList()).isEmpty();
  }
  @Test
  void p2_one() {
    viewsInTopic.pipeInput("user","home");
    testDriver.advanceWallClockTime(Duration.ofSeconds(2));
    assertThat(viewsCountOutTopic.readKeyValuesToMap()).isEqualTo(Map.of("home", 1L));
  }
  @Test
  void p3_notYet() {
    viewsInTopic.pipeInput("user","home");
    assertThat(viewsCountOutTopic.readKeyValuesToMap()).isEmpty();
  }
  @Test
  void p4_twoClose() {
    viewsInTopic.pipeInput("user","home");
    viewsInTopic.pipeInput("user","home");
    testDriver.advanceWallClockTime(Duration.ofSeconds(2));
    assertThat(viewsCountOutTopic.readKeyValuesToMap()).isEqualTo(Map.of("home", 2L));
  }

  @Test
  void p5_twoClose() {
    viewsInTopic.pipeInput("user","home");
    viewsInTopic.pipeInput("user","home");
    testDriver.advanceWallClockTime(Duration.ofSeconds(2));
    assertThat(viewsCountOutTopic.readKeyValuesToMap()).isEqualTo(Map.of("home", 2L));
  }
  @Test
  void pA_alarmOver100() {
    var viewsCountInTopic = testDriver.createInputTopic("page-views-count", String().serializer(), Long().serializer());
    viewsCountInTopic.pipeInput("less", 60L);
    viewsCountInTopic.pipeInput("home", 101L);

    var alarmOutTopic = testDriver.createOutputTopic("page-view-alarm", String().deserializer(), Long().deserializer());
    assertThat(alarmOutTopic.readKeyValuesToList()).containsExactly(
        new KeyValue<>("home", 101L)
    );
  }

  @Test
  void pD_delta() {
    var viewsCountInTopic = testDriver.createInputTopic("page-views-count", String().serializer(), Long().serializer());
    viewsCountInTopic.pipeInput("home", 20L);
    viewsCountInTopic.pipeInput("home", 30L); // + 10
    viewsCountInTopic.pipeInput("home", 30L); // no change -> no event

    var deltaOutTopic = testDriver.createOutputTopic("page-view-delta", String().deserializer(), Long().deserializer());
    assertThat(deltaOutTopic.readKeyValuesToList()).containsExactly(
        new KeyValue<>("home", 10L)
    );
  }



}
