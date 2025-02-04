package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import victor.training.kafka.Event;

import java.util.Map;

@Slf4j
@SuppressWarnings("unused")
public class MyConsumerInterceptor implements ConsumerInterceptor<String, Event> {
  @Override
  public ConsumerRecords<String, Event> onConsume(ConsumerRecords<String, Event> records) {
    log.info("Polled {} records from partitions {}", records.count(),records.partitions());
    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    log.info("Committing offsets: {}", offsets);
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
