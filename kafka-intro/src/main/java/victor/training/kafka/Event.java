package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

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

  @SuppressWarnings("unused") // used in .yaml
  class Serializer extends JsonSerializer<Event> {
    public Serializer() {
      super(JsonUtils.sealedJackson(Event.class));
    }
  }
  @SuppressWarnings("unused") // used in .yaml
  class Deserializer extends JsonDeserializer<Event> {
    public Deserializer() {
      super(JsonUtils.sealedJackson(Event.class));
    }
  }
}
