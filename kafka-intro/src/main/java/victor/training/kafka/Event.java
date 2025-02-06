package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import java.util.UUID;

@JsonTypeInfo(use = Id.NAME)
public sealed interface Event {
  record EventOK(String work) implements Event {
  }
  record EventTakingLong(String work) implements Event {
  }
  record EventCausingError(String work) implements Event {
  }
  record EventForLater(String work, UUID idempotencyKey) implements Event {
  }
}
