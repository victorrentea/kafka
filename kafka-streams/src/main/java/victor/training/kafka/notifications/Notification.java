package victor.training.kafka.notifications;

public record Notification(
    String message,
    String recipientUsername
) {
}
