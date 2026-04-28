package victor.training.library.checkout;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class CheckoutController {
    private final MemberClient memberClient;
    private final TeleportClient teleportClient;

    record CheckoutRequest(long userId, List<Long> bookIds) {}

    @PostMapping("/checkout")
    public void checkout(@RequestBody CheckoutRequest request) {
        String checkoutId = UUID.randomUUID().toString();
        memberClient.addBooks(request.userId(), checkoutId, request.bookIds());
        try {
            teleportClient.teleport(request.userId(), request.bookIds());
        } catch (Exception e) {
            memberClient.removeBooks(request.userId(), checkoutId);
            throw e;
        }
    }
}
