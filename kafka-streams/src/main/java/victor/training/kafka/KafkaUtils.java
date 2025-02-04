package victor.training.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

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
}
