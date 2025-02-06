package victor.training.kafka.notifications;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@RequiredArgsConstructor
@RestController
public class NotificationSSE {
  private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

  @KafkaListener(topics = "send-email", filter = "")
  public void consumeMessage(String message) {
    // Send message to all active SSE clients
    for (SseEmitter emitter : emitters) {
      try {
        emitter.send(SseEmitter.event().data(message));
      } catch (IOException e) {
        log.warn("Unable to write to SSE client: "+e.getMessage());
        emitter.complete();
        emitters.remove(emitter);
      }
    }
  }
  @GetMapping(value = "/notifications",produces = "text/event-stream")
  public SseEmitter registerEmitter() {
    SseEmitter emitter = new SseEmitter(60_000L); // 1-minute timeout
    emitter.onCompletion(() -> emitters.remove(emitter));
    emitter.onTimeout(() -> {
      emitter.complete();
      emitters.remove(emitter);
    });
    emitters.add(emitter);
    return emitter;
  }
}
