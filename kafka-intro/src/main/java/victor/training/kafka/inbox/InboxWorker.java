package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxWorker {
  @SneakyThrows
  public void process(String work) {
    log.info("Processing task: {}", work);
    Thread.sleep(1000);
    if (Math.random() < 0.1) {
      throw new RuntimeException("Error during processing (pretend)");
    }
    log.info("Normal completion of task: {}", work);
  }
}
