package victor.training.kafka.seqno;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class SeqNoListener {
  public static final String TOPIC = "ooo-seqno-in";
  public static final String OUT_TOPIC = "ooo-seqno-out";

  public record SeqMessage(long seqNo, String payload) {}

  private final SeqBufferRepo bufferRepo;
  private final SeqTrackerRepo trackerRepo;
  private final KafkaTemplate<String, String> kafkaTemplate;

  @KafkaListener(topics = TOPIC, concurrency = "1")
  @Transactional
  public void handle(SeqMessage message) {
    log.info("Received seq={} payload={} -> buffering", message.seqNo(), message.payload());

    if (!bufferRepo.existsById(message.seqNo())) {
      bufferRepo.save(new SeqBuffer(message.seqNo(), message.payload()));
    }

    SeqTracker tracker = trackerRepo.findById(1).orElseGet(()->trackerRepo.save(new SeqTracker()));
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
