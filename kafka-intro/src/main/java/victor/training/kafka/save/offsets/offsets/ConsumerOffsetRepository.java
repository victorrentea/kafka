package victor.training.kafka.save.offsets.offsets;

import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.springframework.data.repository.CrudRepository;

public interface ConsumerOffsetRepository extends CrudRepository<ConsumerOffset, Long> {

    Optional<ConsumerOffset> findByTopicAndPartition(String topic, Integer partition);

    default ConsumerOffset find(TopicPartition tp) {
        return findByTopicAndPartition(tp.topic(), tp.partition())
            .orElseGet(() -> ConsumerOffset.atBeginning(tp));
    }
}
