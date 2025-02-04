package victor.training.kafka.notifications;

public record SendEmail(
    String message,
    String recipientEmail
) {
}
