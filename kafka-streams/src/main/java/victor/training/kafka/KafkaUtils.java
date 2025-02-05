package victor.training.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaUtils {
  public static void createTopic(String topicName) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    try (var adminClient = AdminClient.create(properties)) {
      adminClient.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)));
    }
  }

  public static class Ticker implements Processor<String, String, String, String> {
    public static final String DUMMY_VALUE = "tick";
    private final Duration interval;

    public Ticker(Duration interval) {
      this.interval = interval;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
      context.schedule(interval, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
        context.forward(new org.apache.kafka.streams.processor.api.Record<>(DUMMY_VALUE, DUMMY_VALUE, timestamp));
      });
    }

    @Override
    public void process(Record<String, String> record) {
    }
  }

}
