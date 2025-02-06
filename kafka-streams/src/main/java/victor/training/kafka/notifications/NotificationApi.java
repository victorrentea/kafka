package victor.training.kafka.notifications;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import victor.training.kafka.notifications.events.Broadcast;
import victor.training.kafka.notifications.events.Notification;
import victor.training.kafka.notifications.events.UserUpdated;

import java.util.List;

@Slf4j
@RestController
@RequiredArgsConstructor
public class NotificationApi {
  private final KafkaTemplate<String, Notification> notification;
  private final KafkaTemplate<String, UserUpdated> userUpdated;
  private final KafkaTemplate<String, Broadcast> broadcast;

  @EventListener(ApplicationStartedEvent.class)
  public void init() {
    log.info("Publish user updates event(s)");
    userUpdated.send("user-updated","victor",
        new UserUpdated("victor", "victorrentea@gmail.com", true));
    userUpdated.send("user-updated","john",
        new UserUpdated("john", "john@el.com", true));
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
}
