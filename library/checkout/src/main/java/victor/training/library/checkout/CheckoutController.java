package victor.training.library.checkout;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class CheckoutController {
    private final CheckoutService checkoutService;

    @PostMapping("/checkout")
    public void checkout(@RequestBody CheckoutRequest request) {
        checkoutService.checkout(request);
    }
}
