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

  @Scheduled(fixedRate = 1000)
  public void sendBufferedMessagesOlderThanTimeWindow() {
    List<SeqBuffer> old = bufferRepo.findSeqBufferByInsertedAtBefore(LocalDateTime.now().minus(TIME_WINDOW));
    if (old.isEmpty()) {
      return; // nothing to do
    }
    log.info("Scheduled check found {} old buffered messages", old.size());

    // Group old buffered messages by aggregate id
    Map<Integer, List<SeqBuffer>> byAggId = old.stream().collect(groupingBy(b -> b.id().aggId()));

    for (Map.Entry<Integer, List<SeqBuffer>> entry : byAggId.entrySet()) {
      int aggId = entry.getKey();
      List<SeqBuffer> buffers = entry.getValue();
      if (buffers.isEmpty()) continue;

      // Determine the next sequence number to publish for this aggregate
      NextSeq tracker = nextSeqRepo.findById(aggId)
          .orElseGet(() -> nextSeqRepo.save(new NextSeq().aggId(aggId)));

      long nextSeqNo = tracker.nextSeqNo();

      // Choose the smallest available old seqNo that is >= nextSeqNo (skip the gap)
      final long currentNext = nextSeqNo;
      Long firstAvailableSeq = buffers.stream()
          .map(b -> b.id().seqNo())
          .filter(seq -> seq >= currentNext)
          .min(naturalOrder())
          .orElse(null);

      if (firstAvailableSeq == null) continue;

      nextSeqNo = firstAvailableSeq;

      // From that point, publish all consecutive buffered messages if present
      while (true) {
        SeqBuffer buffered = bufferRepo.findById(new AggIdSeqNo(aggId, nextSeqNo)).orElse(null);
        if (buffered == null) break;

        log.info("[timeout] Publishing {} for aggId={}, seqNo={} to {}", buffered.payload(), aggId, nextSeqNo, OUT_TOPIC);
        try {
          kafkaTemplate.send(OUT_TOPIC, buffered.payload()).get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        bufferRepo.delete(buffered);
        nextSeqNo++;
      }

      if (nextSeqNo != tracker.nextSeqNo()) {
        tracker.nextSeqNo(nextSeqNo);
        nextSeqRepo.save(tracker);
      }
    }
  }
}
