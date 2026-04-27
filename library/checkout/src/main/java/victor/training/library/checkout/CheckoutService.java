package victor.training.library.checkout;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CheckoutService {
    private final MemberClient memberClient;
    private final TeleportClient teleportClient;

    public void checkout(CheckoutRequest request) {
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
