package victor.training.kafka.users;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class UsersTopologyTest {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, UsersTopology.User> usersTopic;
  private TestInputTopic<String, String> chatTopic;
  private TestOutputTopic<String, String> outTopic;

  @BeforeEach
  void setup() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "users-topology-test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

    StreamsBuilder builder = new StreamsBuilder();
    UsersTopology.createTopology(builder);
    Topology topology = builder.build();
    testDriver = new TopologyTestDriver(topology, props);

    usersTopic = testDriver.createInputTopic(
        UsersTopology.USERS_TOPIC,
        Serdes.String().serializer(),
        new JsonSerde<>(UsersTopology.User.class).serializer());
    chatTopic = testDriver.createInputTopic(
        UsersTopology.CHAT_LINES_TOPIC,
        Serdes.String().serializer(),
        Serdes.String().serializer());
    outTopic = testDriver.createOutputTopic(
        UsersTopology.CHAT_LINES_RESOLVED_TOPIC,
        Serdes.String().deserializer(),
        Serdes.String().deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void replacesWithAtPrefix() {
    usersTopic.pipeInput("john", new UsersTopology.User("john", "John Doe", "john@example.com"));

    chatTopic.pipeInput(null, "@john hello there");

    List<KeyValue<String, String>> out = outTopic.readKeyValuesToList();
    assertThat(out)
        .containsExactly(new KeyValue<>(null, "John Doe hello there"));
  }

  @Test
  void replacesWithAmpersandPrefix() {
    usersTopic.pipeInput("jane", new UsersTopology.User("jane", "Jane Roe", "jane@example.com"));

    chatTopic.pipeInput(null, "&jane hi!");

    List<KeyValue<String, String>> out = outTopic.readKeyValuesToList();
    assertThat(out)
        .containsExactly(new KeyValue<>(null, "Jane Roe hi!"));
  }

  @Test
  void filtersIfUsernameUnknown() {
    // users table empty
    chatTopic.pipeInput(null, "@ghost hello");

    assertThat(outTopic.isEmpty()).isTrue();
  }

  @Test
  void preservesRemainderWhitespace() {
    usersTopic.pipeInput("tabby", new UsersTopology.User("tabby", "Tab By", "tabby@example.com"));

    chatTopic.pipeInput(null, "@tabby\t\tHello  world");

    List<KeyValue<String, String>> out = outTopic.readKeyValuesToList();
    assertThat(out)
        .containsExactly(new KeyValue<>(null, "Tab By\t\tHello  world"));
  }
}
