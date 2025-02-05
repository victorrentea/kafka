package victor.training.kafka.metrics;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import victor.training.kafka.KafkaUtils;
import victor.training.kafka.notifications.Broadcast;
import victor.training.kafka.notifications.Notification;
import victor.training.kafka.notifications.SendEmail;
import victor.training.kafka.notifications.UserUpdated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
