package victor.training.kafka.consumer.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@ActiveProfiles("test")
@Slf4j
@SpringBootTest
@Import(BaseErrorInConsumerTest.Attempter.class)
public abstract class BaseErrorInConsumerTest {

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private Attempter attempter;
  @BeforeEach
  final void before() {
    attempter.clear();
  }
  protected List<String> attempts() {
    return attempter.attempts();
  }

  @Component
  static class Attempter {
    private List<String> attempts = Collections.synchronizedList(new ArrayList<>());
    public List<String> attempts() { return attempts; }

    public void clear() {
      attempts.clear();
    }
    public void attempt(String event) {
      log.error("Attempting {}", event);
      attempts.add(event);
      throw new RuntimeException("Simulated error");
    }
  }
}
