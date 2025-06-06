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
  private final OutboxRepo outboxRepo;

  @Transactional
  public void flow() throws InterruptedException {
    orderRepo.save(new Order());
    sender.send("Event"); // kafka/rabbit send
  }
  // R/K gets the message, but after exiting the method, the SQL commit fails for some UK violation
  // ❌SQL
  // ✅Message

  public void flow2() throws InterruptedException {
    orderRepo.save(new Order()); // commit
    sender.send("Event"); // kafka/rabbit send
  }
  // SQL commit might work, but Kafka broker might reject (drama!)
  // ✅SQL
  // ❌Message


  // 2PC (2-phase-commit) can orchestrate a distributed commit over 2 DBs or DB+Brokers (Artemis, ActiveMQ,WMQ, MSMQ)
  // hyped in early 2000s
  // died with highly scalable,HA microservices ❌don't use
  // ✅SQL
  // ✅Message
  // or
  // ❌SQL
  // ❌Message
  @Transactional
  public void outbox() throws InterruptedException {
    orderRepo.save(new Order()); // commit
    outboxRepo.save(new Outbox().messageToSend("Event")); // save to outbox table
  }


}
