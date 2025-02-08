package victor.training.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaRawMain {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    // TODO redo
    var producer = createProducer();
    var record = new ProducerRecord<>("hello-topic", "key","value");
    producer.send(record);

    producer.send(new ProducerRecord<>(
        "hello-topic",
        1,
        new Date().getTime() - 100000,
        "key",
        "value2",
        List.of(new RecordHeader("header", "value".getBytes()))));
    System.out.println("Sent 2 messages");

    var consumer = createConsumer();
    consumer.subscribe(List.of("hello-topic"));

    long t0 = System.currentTimeMillis();
    while (System.currentTimeMillis() - t0 < 2000) {
      ConsumerRecords<String, String> receivedRecords = consumer.poll(Duration.ofMillis(100));
      for (var r : receivedRecords) {
        System.out.println(r.key() + " -> " + r.value());
      }
    }
  }

  private static KafkaProducer<String, String> createProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "java");
    return new KafkaProducer<>(props, Serdes.String().serializer(), Serdes.String().serializer());
  }

  private static KafkaConsumer<String, String> createConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "time");
    props.put("internal.leave.group.on.close", "true");
    return new KafkaConsumer<>(props,
        Serdes.String().deserializer(), Serdes.String().deserializer());
  }
}

