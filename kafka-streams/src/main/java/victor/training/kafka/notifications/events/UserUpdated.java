package victor.training.kafka.notifications.events;

public record UserUpdated(
    String username,
    String email,
    boolean acceptsEmailNotifications) {
}
