package victor.training.library.member;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MemberController {
    private final MemberBookRepo repo;

    record AddBooksRequest(String checkoutId, List<Long> bookIds) {}

    @PostMapping("/members/{userId}/books")
    @Transactional
    public void addBooks(@PathVariable long userId, @RequestBody AddBooksRequest request) {
        long existing = repo.countByUserIdAndCheckoutIdNot(userId, request.checkoutId());
        if (existing + request.bookIds().size() > 5) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Member %d already has %d books (max 5)".formatted(userId, existing));
        }
        for (long bookId : request.bookIds()) {
            if (!repo.existsByCheckoutIdAndBookId(request.checkoutId(), bookId)) {
                repo.save(new MemberBook(userId, bookId, request.checkoutId()));
            }
        }
    }

    @DeleteMapping("/members/{userId}/books/{checkoutId}")
    @Transactional
    public void removeBooks(@PathVariable long userId, @PathVariable String checkoutId) {
        repo.deleteByUserIdAndCheckoutId(userId, checkoutId);
    }
}
