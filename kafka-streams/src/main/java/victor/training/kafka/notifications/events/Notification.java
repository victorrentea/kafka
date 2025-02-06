package victor.training.kafka.notifications.events;

public record Notification(
    String message,
    String recipientUsername
) {
}
