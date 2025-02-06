package victor.training.kafka.consumer.error;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Import(RetryOffButDLTTest.ErrorConsumer.class)
public class RetryOffButDLTTest extends BaseErrorInConsumerTest {
  protected String M1 = RandomStringUtils.randomAlphanumeric(5);
  @Autowired
  private ConsumerFactory<String, String> consumerFactory;

  @Test
  void test() throws InterruptedException {
    kafkaTemplate.send("errors-play", M1);

    Thread.sleep(5000);

    assertThat(attempts()).containsExactly(M1);

    System.out.println("Start downloading DLT contents");
    kafkaTemplate.setConsumerFactory(consumerFactory);
    for (int i = 0; i < 50; i++) {
      var record = kafkaTemplate.receive("errors-play-dlt", 0, i, Duration.ofMillis(100));
      if (record.value().equals(M1)) {
        return;
      }
    }
    fail("Message not found in DLT topic (errors-play-dlt): " + M1);
  }


  @TestConfiguration
  static class RetryConfig {
    // TODO No retries but send to a Dead Letter Topic (DLT) called "errors-play-dlt"
//    @Bean TODO
//    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
//      return
//    }
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
