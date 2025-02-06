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
  // nu ma conectez la nici un broker kafka!!
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    Topology topology = WordsTopology.topology();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    //TODO rename phrase-topic
    inputTopic = testDriver.createInputTopic("word-input", Serdes.String().serializer(), Serdes.String().serializer());
    outputTopic = testDriver.createOutputTopic("word-count-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
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
    inputTopic.pipeInput("key", "hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 1L);
  }

  @Test
  void oneWordLower() {
    inputTopic.pipeInput("key", "Hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 1L);
  }

  @Test
  void splitWords() {
    inputTopic.pipeInput("key", "Hi");
    inputTopic.pipeInput("key", "Hello world");

    var wordTopic = testDriver.createOutputTopic("out-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

    assertThat(wordTopic.readValuesToList()).contains("hi", "hello", "world");
  }
  @Test
  void twoWords() {
    inputTopic.pipeInput("key", "Hello world");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 1L)
        .containsEntry("world", 1L);
  }

  @Test
  void twoMessages() {
    inputTopic.pipeInput("key", "Hello World");
    inputTopic.pipeInput("key", "Hello");

    assertThat(outputTopic.readKeyValuesToMap())
        .containsEntry("hello", 2L)
        .containsEntry("world", 1L);
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
