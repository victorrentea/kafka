package victor.training.library.checkout;

import java.util.List;

public record CheckoutRequest(long userId, List<Long> bookIds) {}
