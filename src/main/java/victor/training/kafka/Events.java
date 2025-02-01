package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Builder;
import lombok.With;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@JsonTypeInfo(use= Id.NAME)
sealed interface Event permits Event1, Event2, CheckoutBooks, CheckoutBook, BookCheckedOutEvent, BooksCheckedOutEvent {
}
record Event1(String name) implements Event {
}
record Event2(String age) implements Event {
}

@Builder
record CheckoutBooks(
    UUID eventId,
    String checkoutId,
    String userId,
    List<BookCheckout> checkouts,
    LocalDateTime checkedOutAt,
    String teleportMethod
)  implements Event {

  record BookCheckout(
      String bookId,
      LocalDate returnDate
  ) {}
}

record CheckoutBook(String bookId, String userId, String checkoutId) implements Event {
}

record BookCheckedOutEvent(String bookId, String checkoutId, boolean success)implements Event {
}

record BooksCheckedOutEvent(UUID checkoutId,@With boolean success) implements Event {
}