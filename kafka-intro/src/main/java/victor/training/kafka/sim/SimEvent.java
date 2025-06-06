package victor.training.kafka.sim;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public sealed interface SimEvent {

  Long simId();

  record CreditAdded(Long simId, int credit) implements SimEvent {}

  record OfferActivated(Long simId, String offerId, int price) implements SimEvent {}
}