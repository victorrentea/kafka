package victor.training.kafka.consumer.error;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled
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

  @Slf4j
  @Component
  @RequiredArgsConstructor
  static class ErrorConsumer {
    private final Attempter attempter;

    @KafkaListener(topics = "errors-play")
    @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 1000, multiplier = 1.0, maxDelay = 1000))
    public void consume(String event) {
      attempter.attempt(event);
    }

//    @KafkaListener(topics = "retry-freeze") // TODO
    public void consumeFreezeConsumerThreadOnError(String event, Acknowledgment ack) {
      log.info("Enter consumer for event {}", event);
      try {
        if (Math.random()<.5) {
          throw new RuntimeException("Boom");
        }
      } catch (Exception e) {
        log.info("Stopping consumer thread for event {} due to", event);
        ack.nack(Duration.ofSeconds(1));
      }
      log.info("Exit consumer");
    }
  }
}
