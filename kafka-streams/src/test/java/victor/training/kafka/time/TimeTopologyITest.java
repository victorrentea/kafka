package victor.training.kafka.time;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SuppressWarnings("ALL")
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TimeTopologyITest {

  private static KafkaStreams createKafkaStreams(Topology topology) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1"); // disable caching for faster outcome
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing
    System.out.println(topology.describe());
    KafkaStreams streams = new KafkaStreams(topology, properties);
    streams.start();
    return streams;
  }

  @SneakyThrows
  private static void send(KafkaProducer<String, Long> producer, long value, long timestamp, String key) {
    producer.send(new ProducerRecord<String, Long>(
        "time-input", 0, timestamp, key, value)).get();
  }

  @SneakyThrows
  private static void sendDummy(KafkaProducer<String, Long> producer) {
    producer.send(new ProducerRecord<String, Long>("time-input", 0, null, "dummy", null)).get();
  }

  private static KafkaProducer<String, Long> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "java");
    KafkaProducer<String, Long> producer = new KafkaProducer<>(props, Serdes.String().serializer(), Serdes.Long().serializer());
    return producer;
  }

  private static KafkaConsumer<String, Long> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "time");
    props.put("internal.leave.group.on.close", "true");
    KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props, Serdes.String().deserializer(), Serdes.Long().deserializer());
    consumer.subscribe(List.of("time-tumbling", "time-sliding", "time-hopping", "time-session"));
    return consumer;
  }


  long t0 = System.currentTimeMillis();

  @Test
  void tumbling() throws ExecutionException, InterruptedException {
    experiment(TimeTopology.tumbling());
  }
  @Test
  void sliding() throws ExecutionException, InterruptedException {
    experiment(TimeTopology.sliding());
  }
  @Test
  void hopping() throws ExecutionException, InterruptedException {
    experiment(TimeTopology.hopping());
  }
  @Test
  void session() throws ExecutionException, InterruptedException {
    experiment(TimeTopology.session());
  }

  private void experiment(Topology windowTopology) throws InterruptedException, ExecutionException {
    try (KafkaStreams kafkaStreams = createKafkaStreams(windowTopology);
         KafkaProducer<String, Long> producer = createProducer();
         KafkaConsumer<String, Long> consumer = createConsumer();) {

      CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
        while (System.currentTimeMillis() - t0 < 7_000) {
          ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
          records.forEach(record -> System.out.println(record.topic() + ": " + record.key() + " -> " + record.value()));
        }
      });
      long t0=System.currentTimeMillis();
      for (int i = 0; i < 20; i++) {
//        Thread.sleep(100);
        send(producer,  1000,t0+i*100, "k");
      }
      Thread.sleep(1100);
      send(producer,0, t0+2000+1100, "kkk");
      System.out.println("Sent all");
      future.get();
      System.out.println("Done");
    }
  }

  class TumblingTest {

  }
}
