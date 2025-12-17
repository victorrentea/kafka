package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
class Sender {
  void send(String messageToSend) throws InterruptedException {
    log.info("Sending: {}", messageToSend);
    Thread.sleep(1000);
    if (Math.random() < 0.1) {
      throw new RuntimeException("Error during send");
    }
    log.info("Sent!");
  }
}
