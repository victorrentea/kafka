package victor.training.kafka.time;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.*;
import victor.training.kafka.words.WordsTopology;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@SuppressWarnings("ALL")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TimeTopologyITest {

  private static KafkaStreams kafkaStreams;
  private static KafkaProducer<String, String> producer;
  private static KafkaConsumer<String, String> consumer;

  @BeforeAll
  static void before() {
    kafkaStreams = createKafkaStreams();
    kafkaStreams.start();
    producer = createProducer();
    consumer = createConsumer();
    send("a", Duration.ofSeconds(1));
  }

  @SneakyThrows
  private static void send(String letter, Duration afterDelay) {
    Thread.sleep(afterDelay.toMillis());
    producer.send(new ProducerRecord<String, String>("time-input", "k", letter)).get();
  }

  private static KafkaStreams createKafkaStreams() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing
    StreamsBuilder streams = new StreamsBuilder();
    WordsTopology.createTopology(streams);
    Topology topology = streams.build();
    System.out.println(topology.describe());
    return new KafkaStreams(topology, properties);
  }

  private static KafkaProducer<String, String> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "java");
    return new KafkaProducer<>(props, Serdes.String().serializer(), Serdes.String().serializer());
  }

  private static KafkaConsumer<String, String> createConsumer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "java");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(), Serdes.String().deserializer());
    consumer.subscribe(List.of("time-tumbling"));
    return consumer;
  }

  @AfterAll
  static void after() {
    kafkaStreams.close();
  }

  @Test
  void explore() {
    for (int i = 0; i < 20; i++) {
      send("A", Duration.ofSeconds(1));
    }

//    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
//    assertThat(tumbling.readValuesToList())
//        .containsExactly("AAAA", "AAAAA", "AAAAA", "AAAAA");
//    records.records("time-tumbling")

  }
}
