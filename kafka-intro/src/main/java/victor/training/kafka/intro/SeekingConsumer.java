package victor.training.kafka.intro;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;

import java.util.Map;

//@Component
class SeekingConsumer implements ConsumerSeekAware {
  @Override
  public void onPartitionsAssigned(
      Map<TopicPartition, Long> assignments, // {"myTopic-0"->11000}
      ConsumerSeekCallback callback) {
    for (TopicPartition topicPartition : assignments.keySet()) {
      int newOffset = 100; // from my SQL
      callback.seek(topicPartition.topic(), topicPartition.partition(), newOffset);
    }
  }

  @KafkaListener(topics = "myTopic", groupId = "g1")
  void listen(Event msg) {
    System.out.println(msg);
  }
}