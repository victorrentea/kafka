package victor.training.kafka.sim;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
public sealed interface SimEvent {

  Long simId();

  record AddCredit(Long simId, int credit, String ik) implements SimEvent {}

  record ActivateOffer(Long simId, String offerId, int price) implements SimEvent {}
}