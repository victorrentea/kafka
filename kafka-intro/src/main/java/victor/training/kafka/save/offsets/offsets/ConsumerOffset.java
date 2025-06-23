package victor.training.kafka.save.offsets.offsets;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import org.apache.kafka.common.TopicPartition;

import lombok.Data;

@Entity
@Data
public class ConsumerOffset {
    @Id
    @GeneratedValue
    private Long id;
    private String topic;
    private Long offsetIndex;
    private Integer partition;

    public ConsumerOffset increment() {
        offsetIndex++;
        return this;
    }

    public static ConsumerOffset atBeginning(TopicPartition tp) {
        return new ConsumerOffset()
            .topic(tp.topic())
            .partition(tp.partition())
            .offsetIndex(-1L);
    }
}
