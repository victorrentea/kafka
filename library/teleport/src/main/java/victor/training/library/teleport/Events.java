package victor.training.library.teleport;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, consume BookCheckedOut instead of the REST endpoint

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "teleport")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     teleportController.teleport(new TeleportController.TeleportRequest(event.userId(), event.bookIds()));
    // }
}
