package victor.training.kafka.notifications.events;

public record SendEmail(
    String message,
    String recipientEmail
) {
}
