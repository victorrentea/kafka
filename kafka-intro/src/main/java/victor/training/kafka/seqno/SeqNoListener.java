package victor.training.kafka.seqno;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import victor.training.kafka.seqno.SeqBuffer.AggIdSeqNo;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import static java.util.Comparator.naturalOrder;
import static java.util.stream.Collectors.groupingBy;

@Slf4j
@Component
@RequiredArgsConstructor
public class SeqNoListener {
  public static final String IN_TOPIC = "ooo-seqno-in";
  public static final String OUT_TOPIC = "ooo-seqno-out";
  public static final Duration TIME_WINDOW = Duration.ofSeconds(3);

  public record SeqMessage(int aggId, long seqNo, String payload) {}

  private final SeqBufferRepo bufferRepo;
  private final NextSeqRepo nextSeqRepo;
  private final KafkaTemplate<String, String> kafkaTemplate;


  // TODO victorrentea 2025-11-01: rewrite using in-mem storage only

  @KafkaListener(topics = IN_TOPIC, concurrency = "1")
  @Transactional
  public void handle(SeqMessage message) {
    if (bufferRepo.existsById(new AggIdSeqNo(message.aggId, message.seqNo))) {
      log.warn("Duplicate detected: ignoring " + message);
      return;
    }
    bufferRepo.save(new SeqBuffer(new AggIdSeqNo(message.aggId, message.seqNo), message.payload()));

    NextSeq nextSeq = nextSeqRepo.findById(message.aggId())
        .orElseGet(() -> nextSeqRepo.save(new NextSeq().aggId(message.aggId())));
    log.info("Tracker from DB: {}", nextSeq);
    long nextSeqNo = nextSeq.nextSeqNo();
    while (true) {
      log.info("Look in DB for seqno {}", nextSeqNo);
      SeqBuffer buffered = bufferRepo.findById(new AggIdSeqNo(message.aggId, nextSeqNo)).orElse(null);
      if (buffered == null) break;

      try {
        log.info("Publish {} to {}", buffered.payload(), OUT_TOPIC);
        kafkaTemplate.send(OUT_TOPIC, buffered.payload()).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      bufferRepo.delete(buffered);
      nextSeqNo++;
    }
    if (nextSeqNo != nextSeq.nextSeqNo()) {
      nextSeq.nextSeqNo(nextSeqNo);
      nextSeqRepo.save(nextSeq);
    }
  }

}
