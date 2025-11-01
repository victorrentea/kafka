package victor.training.kafka.intro;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jboss.logging.MDC;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
@RequiredArgsConstructor
public class Producer {
  private final KafkaTemplate<String, Event> kafkaTemplate;

  @GetMapping("produce")
  public String produceEvent() throws ExecutionException, InterruptedException {
    String traceId = "123"; // from the incoming request or TenantId from request header/JWT
    MDC.put("traceId", traceId); // printed in log pattern using %X
    kafkaTemplate.send("myTopic",  new Event.EventOK("Work to be done"));
    // returns CompletableFuture
    return "<a href='/produce-many'>Produce many</a> or <a href='/produce'>one</a>";
  }

  @GetMapping("produce-many")
  public String produceEventMany() {
    for (int i = 0; i < 1000; i++) {
      kafkaTemplate.send("myTopic",  new Event.EventOK("Work to be done"))
          .thenAccept(result -> {
            // runs in the SINGLE Producer thread
            log.info("#1 Message ACKed by broker: {}", result.getRecordMetadata().offset());
          });
    }
    log.info("All messages sent: can I see any 'Message ACKed by broker' after this line?");
    return "<a href='/produce-many'>Produce many</a> or <a href='/produce'>one</a>";
  }
}
