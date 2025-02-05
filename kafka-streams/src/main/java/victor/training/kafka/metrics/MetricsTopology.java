package victor.training.kafka.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import victor.training.kafka.KafkaUtils;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class MetricsTopology {

  public static void main(String[] args) { // vanilla Java (no Spring)
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "metrics");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaUtils.createTopic("page-views");
    KafkaStreams kafkaStreams = new KafkaStreams(topology(), properties);

    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }

  public static Topology topology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KStream<String, String> tickerStream = streamsBuilder.stream("page-views", Consumed.with(String(), String())) // Dummy stream (needed for topology)
        .process(() -> new KafkaUtils.Ticker(Duration.ofMillis(100)));

    // page-views = username -> page viewed
    streamsBuilder.stream("page-views", Consumed.with(String(), String()))
        .peek((key, value) -> log.info("Page viewed: {}", value))
        .merge(tickerStream) // HACK to force the time window to close
        .groupBy((username, page) -> page, Grouped.with(String(), String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
        .count(Materialized.with(String(), Long()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .selectKey((key, value) -> key.key())
        .filter((key, value) -> !Objects.equals(key, KafkaUtils.Ticker.DUMMY_VALUE)) // HACK: skip the dummy value
        .peek((key, value) -> log.info("Page {} viewed {} times", key, value))
        .to("page-views-counter", Produced.with(String(), Long()));
    return streamsBuilder.build();
  }
}
