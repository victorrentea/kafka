package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Value;

import java.util.Objects;
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

class Uite{
  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    Event.EventOK event = new Event.EventOK("ok");
    System.out.println(objectMapper.writeValueAsString(event));
  }
}
