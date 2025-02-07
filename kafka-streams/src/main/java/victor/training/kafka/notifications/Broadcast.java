package victor.training.kafka.notifications;

import java.util.List;

public record Broadcast(
    String message,
    List<String> recipientUsernames) {
}
