package victor.training.kafka.consumer.error;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
@Import(RetrySyncTest.ErrorConsumer.class)
public class RetrySyncTest extends BaseErrorInConsumerTest {
  protected String MESSAGE1 = RandomStringUtils.randomAlphanumeric(5);
  protected String MESSAGE2 = RandomStringUtils.randomAlphanumeric(5);
  @Test
  void no_retry() throws InterruptedException {
    kafkaTemplate.send("errors-play", MESSAGE1);
    Thread.sleep(100);
    kafkaTemplate.send("errors-play", MESSAGE2);

    Thread.sleep(5000);

    assertThat(attempts()).containsExactly(MESSAGE1, MESSAGE1, MESSAGE2, MESSAGE2);
  }

  @TestConfiguration
  static class RetryConfig {
    // TODO allow 1 single retry, after 1 second.
    @Bean
    public DefaultErrorHandler errorHandler() {
      return new DefaultErrorHandler(new FixedBackOff(1000, 1));
    }
    // Note: to configure a single @KafkaListener, use (errorListener="beanName") instead of a global errorHandler
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
