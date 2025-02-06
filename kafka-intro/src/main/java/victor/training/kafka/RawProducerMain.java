package victor.training.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class RawProducerMain {
  public static void main(String[] args) {
    Properties props = new Properties();

    var producer = new KafkaProducer<String, String>(props);

  }
}
