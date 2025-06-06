package victor.training.kafka.intro;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import victor.training.kafka.JsonUtils;

@JsonTypeInfo(use = Id.NAME)
public sealed interface Event {
  record EventOK(String work) implements Event {
  }
  record EventTakingLong(String work) implements Event {
  }
  record EventCausingError(String work) implements Event {
  }
  record EventForLater(String work, String idempotencyKey) implements Event {
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
