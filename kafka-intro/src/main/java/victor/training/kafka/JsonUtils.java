package victor.training.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.JacksonUtils;

public class JsonUtils {

  public static ObjectMapper sealedJackson(Class<?> topClass) {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
    for (Class<?> permittedSubclass : topClass.getPermittedSubclasses()) {
      objectMapper.registerSubtypes(permittedSubclass);
    }
    return objectMapper;
  }


}
