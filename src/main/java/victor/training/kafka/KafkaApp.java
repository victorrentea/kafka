package victor.training.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class KafkaApp {

  public static void main(String[] args) {
    SpringApplication.run(KafkaApp.class, args);
  }


  @Bean // customize the auto-created topic
  public NewTopic myTopic() {
    return TopicBuilder.name("myTopic")
        .partitions(2)
        .build();
  }
}
