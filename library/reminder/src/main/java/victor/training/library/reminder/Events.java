package victor.training.library.reminder;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, consume BookCheckedOut to schedule return reminders

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "reminder")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     // Schedule a reminder for event.userId() to return event.bookIds() in 14 days
    //     log.info("Scheduling return reminder for user {} for books {}", event.userId(), event.bookIds());
    // }
}
