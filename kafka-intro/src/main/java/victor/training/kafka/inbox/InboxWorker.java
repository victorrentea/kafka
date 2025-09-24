package victor.training.kafka.inbox;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
@Service
public class InboxWorker {
  List<String> completedWork = new ArrayList<>();

  @SneakyThrows
  public void process(String work) {
    // TODO undo
    log.info("::START task: {}", work);
    Thread.sleep(1000);
    if (Math.random() < 0.5) {
      throw new RuntimeException("Error during processing (pretend)");
    }
    completedWork.add(work);
    log.info("::END task OK: {}", work);
  }
}
