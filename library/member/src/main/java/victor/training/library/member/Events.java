package victor.training.library.member;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, consume BookCheckedOut instead of the REST endpoint

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "member")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     memberController.addBooks(event.userId(),
    //         new MemberController.AddBooksRequest(event.checkoutId(), event.bookIds()));
    // }
}
