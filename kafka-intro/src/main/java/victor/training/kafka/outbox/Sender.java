package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
class Sender {
  void send(String messageToSend, UUID ik) throws InterruptedException {
    log.info("Sending: {} with Idempotency-Key:{}", messageToSend, ik);
    Thread.sleep(1000);
    if (Math.random() < 0.1) {
      throw new RuntimeException("Error during send");
    }
    log.info("Sent!");
  }
}
