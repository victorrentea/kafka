package victor.training.kafka.seqno;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
@RequiredArgsConstructor
public class SeqNoListener {
  public static final String TOPIC = "ooo-seqno-in";
  public static final String OUT_TOPIC = "ooo-seqno-out";

  private final Map<Long, BufferedSeq> nextSeqNo = new HashMap<>();
  record BufferedSeq(long aggId, int nextSeqNo, String payload) {}

  public record SeqMessage(long aggId, long seqNo, String payload) {}

  private final SeqBufferRepo bufferRepo;
  private final SeqTrackingRepo trackerRepo;
  private final KafkaTemplate<String, String> kafkaTemplate;

  @KafkaListener(topics = TOPIC, concurrency = "1")
  @Transactional
  public void handle(SeqMessage message) {

    if (!bufferRepo.existsById(message.seqNo())) {
      bufferRepo.save(new SeqBuffer(message.seqNo(), message.payload()));
    }

    SeqTracking tracker = trackerRepo.findById(1).orElseGet(()->trackerRepo.save(new SeqTracking()));
    log.info("Got traker {}",tracker);
    long next = tracker.nextSeqNo();
    while (true) {
      log.info("Look in DB for seqno {}", next);
      SeqBuffer buffered = bufferRepo.findById(next).orElse(null);
      if (buffered == null) break;
      // publish
      log.info("Publishing in-order {} to {}", buffered.payload(), OUT_TOPIC);
      try {
        kafkaTemplate.send(OUT_TOPIC, buffered.payload()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      bufferRepo.deleteById(next);
      next++;
    }
    if (next != tracker.nextSeqNo()) {
      tracker.nextSeqNo(next);
      trackerRepo.save(tracker);
    }
  }
}
