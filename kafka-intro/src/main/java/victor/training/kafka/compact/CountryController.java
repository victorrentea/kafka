package victor.training.kafka.compact;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import static victor.training.kafka.compact.CompactTopicConfig.COUNTRY_TOPIC;


@RestController
@RequiredArgsConstructor
public class CountryController {
  private final KafkaTemplate<String, String> kafkaTemplate;

  @PostMapping("countries/{iso}")
  public void createCountry(@PathVariable String iso, @RequestParam String name) {
    kafkaTemplate.send(COUNTRY_TOPIC, iso, name);
  }

  @PutMapping("countries/{iso}")
  public void updateCountry(@PathVariable String iso, @RequestParam String name) {
    kafkaTemplate.send(COUNTRY_TOPIC, iso, name);
  }

  @DeleteMapping("countries/{iso}")
  public void deleteCountry(@PathVariable String iso) {
    kafkaTemplate.send(COUNTRY_TOPIC, iso, null);
  }

}
