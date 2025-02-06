package victor.training.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class RawProducerMain {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG,"java");

    var producer = new KafkaProducer<String, String>(props,
        Serdes.String().serializer(), Serdes.String().serializer());
    ProducerRecord<String, String> record = new ProducerRecord<>(
        "quickstart-events2",
        1,
        new Date().getTime()-100000,"k",
        "din Java",
        List.of (new RecordHeader("header1", "value1".getBytes())));
    var future = producer.send(record);
    future.get();
  }
}
