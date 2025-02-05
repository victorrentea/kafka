package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

@JsonTypeInfo(use = Id.NAME)
public sealed interface Event {
  record Event1(String name) implements Event {
  }
  record Event2(String age) implements Event {
  }
}
