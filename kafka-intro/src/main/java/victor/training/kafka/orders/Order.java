package victor.training.kafka.orders;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.*;

@Data
@Entity
@Table(name = "ORDERS",
    uniqueConstraints = @UniqueConstraint(columnNames = "idempotency_key"))
public class Order {
  @Id
  @GeneratedValue// from a sequence
  private Long id; // numeric, as usual
  private String idempotencyKey; // from client
  private String data;
}
