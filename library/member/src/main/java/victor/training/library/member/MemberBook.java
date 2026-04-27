package victor.training.library.member;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"checkoutId", "bookId"}))
public class MemberBook {
    @Id @GeneratedValue
    private Long id;
    private long userId;
    private long bookId;
    private String checkoutId;

    public MemberBook(long userId, long bookId, String checkoutId) {
        this.userId = userId;
        this.bookId = bookId;
        this.checkoutId = checkoutId;
    }
}
