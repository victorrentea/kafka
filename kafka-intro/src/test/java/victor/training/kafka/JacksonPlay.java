package victor.training.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.support.JacksonUtils;

import static victor.training.kafka.Event.EventTakingLong;
import static victor.training.kafka.Event.EventCausingError;

public class JacksonPlay {
  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();
    String s = objectMapper.writeValueAsString(new EventTakingLong("John"));
    System.out.println(s);

    objectMapper.registerSubtypes(EventTakingLong.class, EventCausingError.class);
    Event e = objectMapper.readValue(s, Event.class);
    System.out.println(e);
  }
}

