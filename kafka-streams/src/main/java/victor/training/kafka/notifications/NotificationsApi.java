package victor.training.kafka.notifications;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class NotificationsApi {
  private final KafkaTemplate<String, Notification> notification;
  private final KafkaTemplate<String, UserUpdated> userUpdated;
  private final KafkaTemplate<String, Broadcast> broadcast;

  @EventListener(ApplicationStartedEvent.class)
  @Scheduled(fixedRate = 2000)
  public void init() {
    System.out.println("Sending UserUpdated event");
    this.userUpdated.send("user-updated","victor", new UserUpdated("victor", "victorrentea@gmail.com", true));
    this.userUpdated.send("user-updated","john", new UserUpdated("john", "john@el.com", true));
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
  public void onEmailSent(SendEmail emailSent) {
    System.out.println("✅Email sent: " + emailSent);
  }
}
