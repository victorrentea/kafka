package victor.training.kafka.intro;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

//@Component
class SeekingConsumer implements ConsumerSeekAware {
    private ConsumerSeekCallback seekCallback;

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
      for (Map.Entry<TopicPartition, Long> entry : assignments.entrySet()) {
        var tp = entry.getKey();
        callback.seek(tp.topic(), tp.partition(), 100);
      }
    }

    @KafkaListener(topics = "myTopic", groupId = "g1")
    void listen(Event msg) {
        System.out.println(msg);
    }
}