package victor.training.kafka.schema;

public record LoanRequestedEvent(String firstName, String lastName, int amount, String phone) {
}
