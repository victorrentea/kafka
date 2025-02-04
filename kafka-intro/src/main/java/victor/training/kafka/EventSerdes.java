package victor.training.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@SuppressWarnings("unchecked")
public class EventSerdes {
  private static ObjectMapper polymorphicJackson() {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
    for (Class<?> permittedSubclass : Event.class.getPermittedSubclasses()) {
      objectMapper.registerSubtypes(permittedSubclass);
    }
    return objectMapper;
  }

  public static class EventSerializer extends JsonSerializer<Event> {
    public EventSerializer() {
      super(polymorphicJackson());
    }
  }
  public static class EventDeserializer extends JsonDeserializer<Event> {
    public EventDeserializer() {
      super(polymorphicJackson());
    }
  }

  public static <T extends Event> Serde<T> serde() {
    return Serdes.serdeFrom((Serializer<T>)new EventSerializer(), (Deserializer<T>) new EventDeserializer());
  }
  public static <T extends Event> Serde<T>  serdeFor(Class<T> clazz) {
    return Serdes.serdeFrom((Serializer<T>)new EventSerializer(), (Deserializer<T>) new EventDeserializer());
  }
}
