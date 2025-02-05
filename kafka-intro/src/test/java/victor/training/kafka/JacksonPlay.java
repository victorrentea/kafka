package victor.training.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.JacksonUtils;

import static victor.training.kafka.Event.Event1;
import static victor.training.kafka.Event.Event2;

public class JacksonPlay {
  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
    String s = objectMapper.writeValueAsString(new Event1("John"));
    System.out.println(s);

    objectMapper.registerSubtypes(Event1.class, Event2.class);
    Event e = objectMapper.readValue(s, Event.class);
    System.out.println(e);
  }
}

