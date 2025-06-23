package victor.training.kafka.offer;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

public interface OfferRepo extends JpaRepository<Offer, Long> {
    List<Offer> findByNameContaining(String prefix);
}
