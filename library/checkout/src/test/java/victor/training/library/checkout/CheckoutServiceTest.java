package victor.training.library.checkout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CheckoutServiceTest {
    @Mock MemberClient memberClient;
    @Mock TeleportClient teleportClient;
    @InjectMocks CheckoutService checkoutService;

    @Test
    void happyPath_doesNotCallCompensation() {
        checkoutService.checkout(new CheckoutRequest(1L, List.of(1L, 6L)));

        verify(memberClient).addBooks(eq(1L), any(String.class), eq(List.of(1L, 6L)));
        verify(teleportClient).teleport(eq(1L), eq(List.of(1L, 6L)));
        verify(memberClient, never()).removeBooks(anyLong(), anyString());
    }

    @Test
    void compensation_removesBooks_whenTeleportFails() {
        doThrow(new RuntimeException("teleporter malfunction"))
                .when(teleportClient).teleport(anyLong(), anyList());

        assertThatThrownBy(() -> checkoutService.checkout(new CheckoutRequest(1L, List.of(1L, 6L))))
                .isInstanceOf(RuntimeException.class);

        verify(memberClient).addBooks(eq(1L), any(String.class), eq(List.of(1L, 6L)));
        verify(memberClient).removeBooks(eq(1L), any(String.class));
    }
}
