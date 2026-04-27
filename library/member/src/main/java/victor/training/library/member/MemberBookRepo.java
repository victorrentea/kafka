package victor.training.library.member;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MemberBookRepo extends JpaRepository<MemberBook, Long> {
    long countByUserId(long userId);
    boolean existsByCheckoutIdAndBookId(String checkoutId, long bookId);
    void deleteByUserIdAndCheckoutId(long userId, String checkoutId);
}
