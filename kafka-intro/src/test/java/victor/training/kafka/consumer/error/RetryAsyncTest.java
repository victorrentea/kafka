package victor.training.kafka.consumer.error;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import static org.assertj.core.api.Assertions.assertThat;

@Import(RetryAsyncTest.ErrorConsumer.class)
public class RetryAsyncTest extends BaseErrorInConsumerTest {
  protected String M1 = RandomStringUtils.randomAlphanumeric(5);
  protected String M2 = RandomStringUtils.randomAlphanumeric(5);

  @Test
  void test() throws InterruptedException {
    kafkaTemplate.send("errors-play", M1);
    Thread.sleep(100);
    kafkaTemplate.send("errors-play", M2);

    Thread.sleep(5000);

    assertThat(attempts()).containsExactly(M1, M2, M1, M2);
  }

  @Component
  @RequiredArgsConstructor
  static class ErrorConsumer {
    private final Attempter attempter;

    @KafkaListener(topics = "errors-play")
    public void consume(String event) {
      attempter.attempt(event);
    }
  }
}
