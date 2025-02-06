package victor.training.kafka.notifications.events;

import java.util.List;

public record Broadcast(
    String message,
    List<String> recipientUsernames) {
}
