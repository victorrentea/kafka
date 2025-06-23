package victor.training.kafka.save.offsets;

import java.util.concurrent.ThreadLocalRandom;

import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import victor.training.kafka.offer.Offer;
import victor.training.kafka.offer.OfferRepo;

@Slf4j
@Component
@Profile("save-offsets")
@RequiredArgsConstructor
public class OfferConsumer {

    public static final double POST_PROCESSING_FAILURE_PROBABILITY = 0.3;
    public static final String TOPIC = "create.offer.command";

    private final OfferRepo offers;

    @KafkaListener(topics = TOPIC)
    public void onMessageReceived(String offerName) {

        Offer offer = new Offer().name(offerName);
        offer = offers.save(offer);
        log.info("Saved to DB new offer: {}", offer);

        // simulate post processing failure
        if (ThreadLocalRandom.current()
            .nextDouble(0.0, 1.0) < POST_PROCESSING_FAILURE_PROBABILITY) {
            throw new RuntimeException("Ooupss...!! Simulating failure to acknowledge message: " + offerName);
        }
    }

}
