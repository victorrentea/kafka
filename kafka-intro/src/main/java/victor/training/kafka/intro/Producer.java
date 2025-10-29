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
@Profile("!test")
public class Producer {


  private final KafkaTemplate<String, Event> kafkaTemplate;

  @GetMapping("produce")
  public String produceEvent() throws ExecutionException, InterruptedException {
    MDC.put("traceId", "123");
    kafkaTemplate.send("myTopic", new Event.EventOK("Work to be done")).get();
    return "<a href='/produce-many'>Produce many</a> or <a href='/produce'>one</a>";
  }

  @GetMapping("produce-many")
  public String produceEventMany() {
    // http filter
    String fleetId = "123"; // il iei din JWT/request header/payload/
    MDC.put("traceId", fleetId); // apare automat in log cu %X

    for (int i = 0; i < 1000; i++) {
      kafkaTemplate.send("myTopic", new Event.EventOK("Work to be done"))
          .thenAccept(result -> {
            // in threadul unic al producerului
            log.info("Trimis mesaj cu offset: " + result.getRecordMetadata().offset());
          });
    }
    log.info("All messages sent: se vad in log unele trimise dupa linia asta?");
    return "<a href='/produce-many'>Produce many</a> or <a href='/produce'>one</a>";
  }
}
