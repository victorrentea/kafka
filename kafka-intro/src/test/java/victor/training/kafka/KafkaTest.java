package victor.training.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import victor.training.kafka.sim.SimEvent;

@SpringBootTest
//@EmbeddedKafka // or via Kafka from docker-compose.yaml
public abstract class KafkaTest {
}
