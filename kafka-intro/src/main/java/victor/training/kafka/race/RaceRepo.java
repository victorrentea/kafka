package victor.training.kafka.race;

import org.springframework.data.jpa.repository.JpaRepository;

public interface RaceRepo extends JpaRepository<RaceEntity, String> {
}
