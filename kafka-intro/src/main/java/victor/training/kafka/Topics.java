package victor.training.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Topics {
  @Bean
  public NewTopic myTopic() {
    return TopicBuilder.name("myTopic")
        .partitions(2)
        .replicas(2)
//        .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
        .build();
  }

}