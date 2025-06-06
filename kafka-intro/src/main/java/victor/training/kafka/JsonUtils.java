package victor.training.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.JacksonUtils;
import victor.training.kafka.sim.SimEvent;
import victor.training.kafka.sim.SimEventListener;

public class JsonUtils {

  public static ObjectMapper sealedJackson(Class<?> topSealedType) {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
    // JsonDeserializer.TRUSTED_PACKAGES = victor.*
    for (Class<?> permittedSubclass : topSealedType.getPermittedSubclasses()) {
      objectMapper.registerSubtypes(permittedSubclass);
    }
    return objectMapper;
  }


  public static void main(String[] args) throws JsonProcessingException {
    SimEvent simEvent = new SimEvent.CreditAdded(1L, 100);
    String json = JsonUtils.sealedJackson(SimEvent.class).writeValueAsString(simEvent);
    SimEvent parsed = JsonUtils.sealedJackson(SimEvent.class).readValue(json, SimEvent.class);
    System.out.println("Parsed: " + parsed);
  }

}
