package victor.training.library.checkout;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, publish BookCheckedOut instead of calling Member + Teleport directly

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @Autowired KafkaTemplate<String, BookCheckedOut> kafkaTemplate;
    //
    // Replace the try/catch in CheckoutService with:
    //   kafkaTemplate.send("book-checked-out", checkoutId,
    //       new BookCheckedOut(checkoutId, request.userId(), request.bookIds()));
}
