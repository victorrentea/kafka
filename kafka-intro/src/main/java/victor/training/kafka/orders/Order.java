package victor.training.kafka.orders;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

@Data
@Entity
@Table(name = "ORDERS")
public class Order {
  @Id
  @GeneratedValue
  private Long id;
  private String data;
}
