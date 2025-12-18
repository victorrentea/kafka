package victor.training.kafka.words;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class WordsTopologyTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    StreamsBuilder streams = new StreamsBuilder();
    WordsTopology.createTopology(streams);
    Topology topology = streams.build();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    inputTopic = testDriver.createInputTopic(WordsTopology.WORDS_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic = testDriver.createOutputTopic(
        WordsTopology.WORD_COUNT_TOPIC,
        Serdes.String().deserializer(),
        Serdes.Long().deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void empty() {
    List<KeyValue<String, Long>> records = outputTopic.readKeyValuesToList();
    assertThat(records).isEmpty();
  }

  static List<TestCase> data() {
    return List.of(
        new TestCase(List.of("hello"), List.of(new KeyValue<>("hello", 1L))),
        new TestCase(List.of("Hello"), List.of(new KeyValue<>("hello", 1L))),
        new TestCase(List.of("Hello world"), List.of(new KeyValue<>("hello", 1L), new KeyValue<>("world", 1L))),
        new TestCase(List.of("Hello World", "Hello"), List.of(new KeyValue<>("hello", 1L), new KeyValue<>("world", 1L), new KeyValue<>("hello", 2L)))
    );
  }
  record TestCase(List<String> inputValues, List<KeyValue<String, Long>> expectedRecords) {
//    TestCase out(String key, String value) {
//      ex
//    }
  }

  @ParameterizedTest
  @MethodSource("data")
  void genericTest(TestCase testCase) {
    for (String value : testCase.inputValues) {
      inputTopic.pipeInput("key", value);
    }
    List<KeyValue<String, Long>> counts = outputTopic.readKeyValuesToList();
    assertThat(counts)
        .containsExactlyElementsOf(testCase.expectedRecords());
  }

  @Test
  @Disabled("only for the strong of heart")
  void realMessages() {
    inputTopic.pipeInput("key", "Hello World");
    inputTopic.pipeInput("key", "Hello");

    assertThat(outputTopic.readKeyValuesToList()).containsExactly(
        // TODO observe and explain the actual value
    );
  }
}
