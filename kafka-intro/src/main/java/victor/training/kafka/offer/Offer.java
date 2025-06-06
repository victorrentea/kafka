package victor.training.kafka.offer;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.*;

@Data
@Entity
public class Offer {
  @Id
  @GeneratedValue
  private Long id;
  private String name;
}
