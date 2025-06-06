package victor.training.kafka.sim;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class Sim {
  @Id
  @GeneratedValue
  private Long id;

  private String phoneNumber;

  private boolean active = true;

  private int credit = 0;

  private String activeOfferId;

}

