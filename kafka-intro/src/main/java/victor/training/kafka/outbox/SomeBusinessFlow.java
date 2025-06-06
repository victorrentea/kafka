package victor.training.kafka.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import victor.training.kafka.orders.Order;
import victor.training.kafka.orders.OrderRepo;

@Slf4j
@RequiredArgsConstructor
@Service
public class SomeBusinessFlow {
  private final Sender sender;
  private final OrderRepo orderRepo;

  @Transactional
  public void flow() throws InterruptedException {
    orderRepo.save(new Order());
    sender.send("Event");
  }
}
