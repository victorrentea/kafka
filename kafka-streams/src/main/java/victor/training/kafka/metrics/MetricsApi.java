package victor.training.kafka.metrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

@Slf4j
@RestController
@RequiredArgsConstructor
public class MetricsApi {
  private final KafkaTemplate<String, String> pageViews;
  @GetMapping("/page")
  public void viewPage() {
    pageViews.send("page-views", "username", "page");
  }

  @Autowired
  void buildPipeline(StreamsBuilder streamsBuilder) {
    KafkaUtils.createTopic("page-views-count");
    streamsBuilder
        .stream("page-views-count", Consumed.with(Serdes.String(), Serdes.Long()))
        .foreach((k, v) -> log.info("Page view count: {} for page {}", v, k));
  }
}
