package victor.training.kafka.notifications;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class NotificationsApi {
  private final KafkaTemplate<String, Notification> notification;
  private final KafkaTemplate<String, UserUpdated> userUpdated;
  private final KafkaTemplate<String, Broadcast> broadcast;

  @EventListener(ApplicationStartedEvent.class)
  public void init() {
    log.info("Auto user updates event(s)");
    userUpdated.send("user-updated","victor", new UserUpdated("victor", "victorrentea@gmail.com", true));
    userUpdated.send("user-updated","john", new UserUpdated("john", "john@el.com", true));
  }
  @PostMapping("/notification")
  public void sendNotification() {
    notification.send("notification", new Notification("Hello", "victor"));
  }
  @PostMapping("/user")
  public void updateUser(@RequestBody(required = false) UserUpdated userUpdated) {
    if (userUpdated == null) {
      userUpdated = new UserUpdated("victor", "victorrentea@gmail.com", true);
    }
    this.userUpdated.send("user-updated", "victor", userUpdated);
  }
  @GetMapping("/broadcast")
  public void broadcast(@RequestParam(defaultValue = "victor,john") List<String> usernames) {
    System.out.println("Broadcasting to: " + usernames);
    broadcast.send("broadcast", "b", new Broadcast("Hello", usernames));
  }
  @KafkaListener(topics = "send-email")
  public void onEmailSent(SendEmail emailSent) throws IOException {
    String message = "✅Email sent: " + emailSent;
    System.out.println(message);
    for (SseEmitter emitter : sseEmitters) {
      try {
        emitter.send(message);
      } catch (IOException e) {
        // ignored
      }
    }
  }

  private final List<SseEmitter> sseEmitters = Collections.synchronizedList(new ArrayList<>());
  @GetMapping(value = "/tail",produces = "text/event-stream")
  public SseEmitter tail() {
    SseEmitter sseEmitter = new SseEmitter();
    sseEmitters.add(sseEmitter);
    return sseEmitter;
  }
}
