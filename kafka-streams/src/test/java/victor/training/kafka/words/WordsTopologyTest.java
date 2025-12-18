package victor.training.kafka.words;

import lombok.ToString;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class WordsTopologyTest {
  private TopologyTestDriver driver;
  private TestInputTopic<String, String> inputTopic;
  private TestInputTopic<String, String> dictionaryTopic;
  private TestOutputTopic<String, Long> outputTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-" + UUID.randomUUID());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, String().getClass());
    StreamsBuilder streams = new StreamsBuilder();
    WordsTopology.createTopology(streams);
    Topology topology = streams.build();
    System.out.println(topology.describe());
    driver = new TopologyTestDriver(topology, props);
    inputTopic = driver.createInputTopic(WordsTopology.WORDS_TOPIC, String().serializer(), String().serializer());
    outputTopic = driver.createOutputTopic(WordsTopology.WORD_COUNT_TOPIC, String().deserializer(), Long().deserializer());

//    dictionaryTopic = driver.createInputTopic("dictionary", String().serializer(), String().serializer());
//    dictionaryTopic.pipeInput("halo", "hell");
//    dictionaryTopic.pipeInput("halo", "hello");
  }

  @AfterEach
  final void after() {
    driver.close();
  }

  @Test
  void t1_empty() {
    // no input =>
    assertThat(outputTopic.readKeyValuesToList()).isEmpty();
  }
  @Test
  void t2_one_word() {
    inputTopic.pipeInput("?", "word");

    assertThat(outputTopic.readKeyValuesToList())
        .containsExactly(new KeyValue<>("word", 1L));
  }

  static List<TestCase> data() {
    return List.of(
        new TestCase("hello").expect("hello", 1),
        new TestCase("Hello").expect("hello", 1),
        new TestCase("Hello world")
            .expect("hello", 1)
            .expect("world", 1),

        new TestCase("Hello World", "Hello")
            .expect("hello", 1)
            .expect("world", 1)
            .expect("hello", 2)
//        new TestCase("Halo").expect("hello", 1),
    );
  }

  @ToString
  static final class TestCase {
    final List<String> inputValues;
    final List<KeyValue<String, Long>> expectedRecords = new ArrayList<>();

    TestCase(String... inputValues) {
      this.inputValues = Arrays.asList(inputValues);
    }
    TestCase expect(String key, int value) {
      expectedRecords.add(new KeyValue<>(key, (long)value));
      return this;
    }
  }

  @ParameterizedTest
  @MethodSource("data")
  void t3_genericTest(TestCase testCase) {
    for (String value : testCase.inputValues) {
      inputTopic.pipeInput("key", value);
    }
    List<KeyValue<String, Long>> counts = outputTopic.readKeyValuesToList();
    assertThat(counts)
        .containsExactlyElementsOf(testCase.expectedRecords);
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
