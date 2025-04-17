package victor.training.kafka.words;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

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
    outputTopic = testDriver.createOutputTopic(WordsTopology.WORD_COUNT_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void empty() {
    List<KeyValue<String, Long>> strings = outputTopic.readKeyValuesToList();
    assertThat(strings).isEmpty();
  }

  @Test
  void oneWord() {
    inputTopic.pipeInput("userId", "HelLo Hello World");
    inputTopic.pipeInput("userId", "Hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 2L)
        .containsEntry("world", 1L)
    ;
  }

  @Test
  void oneWordLower() {
    inputTopic.pipeInput("userId", "Hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 1L);
  }

  @Test
  void twoWords() {
    inputTopic.pipeInput("userId", "Hello world");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 1L)
        .containsEntry("world", 1L);
  }
  @Test
  @Disabled
  void oare() {
    inputTopic.pipeInput("userId", "Hello hello world");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 2L)
        .containsEntry("world", 1L);
  }

  @Test
  void twoMessages() {
    inputTopic.pipeInput("userId", "Hello World");
    inputTopic.pipeInput("userId", "Hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 2L)
        .containsEntry("world", 1L);
  }

  @Test
  @Disabled("only for the strong of heart")
  void realMessages() {
    inputTopic.pipeInput("userId", "Hello World");
    inputTopic.pipeInput("userId", "Hello");

    assertThat(outputTopic.readKeyValuesToList()).containsExactly(
        // TODO observe and explain the actual value
    );
  }
}
