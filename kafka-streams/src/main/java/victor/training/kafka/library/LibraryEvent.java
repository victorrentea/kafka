package victor.training.kafka.library;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Builder;
import lombok.With;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@JsonTypeInfo(use = Id.NAME)
public sealed interface LibraryEvent {
  @Builder
  record CheckoutInitiated(
      UUID checkoutId,
      String userId,
      List<BookCheckout> checkouts,
      LocalDateTime checkedOutAt,
      String teleportMethod
  ) implements LibraryEvent {

    record BookCheckout(
        String bookId,
        LocalDate returnDate
    ) {
    }
  }

  record CheckoutBook(String bookId, String userId, UUID checkoutId) implements LibraryEvent {
  }

  record BookCheckedOut(String bookId, UUID checkoutId, boolean success) implements LibraryEvent {
  }

  record CheckoutCompleted(UUID checkoutId, @With List<String> bookIds) implements LibraryEvent {
  }
}
