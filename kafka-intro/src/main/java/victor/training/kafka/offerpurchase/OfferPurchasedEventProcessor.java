package victor.training.kafka.offerpurchase;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import victor.training.kafka.offer.OfferEventListener;
import victor.training.kafka.sim.SimEvent;
import victor.training.kafka.sim.SimEvent.CreditAdded;
import victor.training.kafka.sim.SimEvent.OfferActivated;
import victor.training.kafka.sim.SimEventListener;

@Slf4j
@RequiredArgsConstructor
@Service
public class OfferPurchasedEventProcessor {
  public static final String OFFER_PURCHASED_TOPIC = "offer-purchased-topic";
  private final KafkaTemplate<?, SimEvent> kafkaTemplate;

  @KafkaListener(topics = OFFER_PURCHASED_TOPIC) // recharge + activate
  @Transactional
  public void onOfferPurchased(OfferPurchasedEvent event) {
    kafkaTemplate.send(SimEventListener.SIM_TOPIC, new CreditAdded(event.simId(), event.pricePaid()));
    if(true) throw new IllegalArgumentException("k8s decided to kill me");
    kafkaTemplate.send(SimEventListener.SIM_TOPIC, new OfferActivated(event.simId(), event.offerId(), event.pricePaid()));
  }
  // ATOMIC = {consumer offset++, send, send}
}
