package victor.training.kafka.notifications;

public record UserUpdated(
    String username,
    String email,
    boolean acceptsEmailNotifications) {
}
