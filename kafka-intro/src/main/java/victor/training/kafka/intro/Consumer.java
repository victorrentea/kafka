package victor.training.kafka.intro;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
//import victor.training.kafka.inbox.Inbox;
//import victor.training.kafka.inbox.InboxRepo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {

  @KafkaListener(topics = "myTopic")
  public void consume(ConsumerRecord<String, Event> record) throws InterruptedException {
    log.info("Process " + record);
  }

  @SneakyThrows
  private void anafCall() {
    Thread.sleep(61_000);
  }

  @Bean
  public NewTopic myTopic() {
    return TopicBuilder.name("myTopic")
        .partitions(2)
        .replicas(2)
//        .config(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")
        .build();
  }
}

