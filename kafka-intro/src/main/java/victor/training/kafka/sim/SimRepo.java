package victor.training.kafka.sim;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SimRepo extends JpaRepository<Sim, Long> {
}
