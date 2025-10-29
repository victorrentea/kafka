package victor.training.kafka.intro;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.context.annotation.Bean;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
//import victor.training.kafka.inbox.Inbox;
//import victor.training.kafka.inbox.InboxRepo;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {
  @KafkaListener(topics = "myTopic")
  public void consumeOne(ConsumerRecord<String, Event> record,
                         Acknowledgment ack) throws InterruptedException {
    log.info("Process: " + record);
    ack.acknowledge();
  }

//  @KafkaListener(topics = "myTopic")
//  @Transactional(transactionManager = "kafkaTransactionManager")
//  public void consumeBatch(ConsumerRecords<String, Event> recordBatch) throws InterruptedException {
//    var records = StreamSupport.stream(recordBatch.records("myTopic").spliterator(), false).toList();
////    Thread.sleep(500);
//    log.info("Process: " + records);
//
//    // enrich messages ("READ"): vin orderId: 1,6,7,4,8,
////    List<orderIds>orderApi.getOne(orderIds)
//
//    //repo.save() // SQL nu e tranzactat cu kafka
//    // nu e side effecting (WRITE): nu trimiti emailuri
////    for (order: orders)
////       if (vreau) kafkaTemplate.send();
//  }

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

