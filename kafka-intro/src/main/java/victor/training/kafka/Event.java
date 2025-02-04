package victor.training.kafka;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import lombok.Builder;
import lombok.With;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@JsonTypeInfo(use = Id.NAME)
public sealed interface Event {
  record Event1(String name) implements Event {
  }

  record Event2(String age) implements Event {
  }

}
