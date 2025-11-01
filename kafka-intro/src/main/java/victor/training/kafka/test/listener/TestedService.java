package victor.training.kafka.test.listener;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class TestedService {
  private final ARepo aRepo;

  public void logic(String message) {
    aRepo.save(message);
    // more logic
  }
}
