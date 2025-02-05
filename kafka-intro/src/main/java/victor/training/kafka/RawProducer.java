package victor.training.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class RawProducer {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "victor.training.kafka.EventSerdes$EventSerializer");

    try (KafkaProducer<String, Event> producer = new KafkaProducer<>(props)) {
//      producer.send(new ProducerRecord<>("myTopic", new Event.EventTakingLong("Hello")));
      producer.send(new ProducerRecord<>("myTopic", new Event.EventForLater("background")));
    }
  }
}
