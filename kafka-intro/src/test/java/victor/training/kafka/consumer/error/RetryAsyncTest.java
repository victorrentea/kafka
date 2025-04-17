package victor.training.kafka.consumer.error;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@Import(RetryAsyncTest.ErrorConsumer.class)
public class RetryAsyncTest extends BaseErrorInConsumerTest {
  protected String M1 = RandomStringUtils.randomAlphanumeric(5);
  protected String M2 = RandomStringUtils.randomAlphanumeric(5);

  @SneakyThrows
  @Test
  void test() throws InterruptedException {

    kafkaTemplate.send("errors-play", "K", M1);// mesajul in spate poate crapa la trimitere

    kafkaTemplate.send("errors-play", "K", M1).get();// mesajul in spate poate crapa la trimitere
//    api.call()
    // faci ceva..
    kafkaTemplate.send("errors-play", "K", M1);
//        .thenAccept(r -> api.call()); // in ACEL THREAD UNIC AL KAFKA SENDER
//        .thenAcceptAsync(r -> api.call(), executoruMeu); // pe al meu

    CompletableFuture<SendResult<String, String>> f = kafkaTemplate.send("errors-play", "K", M1);
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
    @RetryableTopic(attempts = "2", backoff = @Backoff(delay = 1000, multiplier = 1.0, maxDelay = 1000))
    public void consume(String event) {
      attempter.attempt(event);
    }
  }
}
