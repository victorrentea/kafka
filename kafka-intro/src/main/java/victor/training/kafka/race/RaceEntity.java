package victor.training.kafka.race;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Version;
import lombok.Data;

import java.util.UUID;

@Entity
@Data
public class RaceEntity {
  @Id
  String id = UUID.randomUUID().toString();

  Integer total = 0;

//  @Version
//  Long version;
}
