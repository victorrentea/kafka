package victor.training.kafka.metrics;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonSerde;
import victor.training.kafka.KafkaUtils;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@SuppressWarnings("ALL")
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

    KStream<String, String> tickerStream = streamsBuilder.stream("page-views", Consumed.with(String(), String())) // Dummy stream (needed for createTopology)
        .process(() -> new KafkaUtils.Ticker(Duration.ofMillis(100)));

    // page-views = username -> page viewed
    streamsBuilder.stream("page-views", Consumed.with(String(), String()))
        .peek((key, value) -> log.info("Page viewed: {}", value))
        .merge(tickerStream) // HACK to advance the stream time so the time window closes
        .groupBy((username, page) -> page, Grouped.with(String(), String()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
        .count(Materialized.with(String(), Long()))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .selectKey((key, value) -> key.key())
        .filterNot((key, value) -> KafkaUtils.Ticker.isDummy(key)) // HACK: skip the dummy value
        .peek((key, value) -> log.info("Page {} viewed {} times", key, value))
        .to("page-views-count", Produced.with(String(), Long()));

    streamsBuilder.stream("page-views-count", Consumed.with(String(), Long()))
        .filter((key, value) -> value > 100)
        .to("page-view-alarm", Produced.with(String(), Long()));


    streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("page-views-previous"), String(), Long()));

    streamsBuilder.stream("page-views-count", Consumed.with(String(), Long()))
        .groupByKey()
        .aggregate(() -> new Tup(-1,0),
            (key, value, aggregate) -> new Tup(value, value - aggregate.previous),
            Materialized.with(String(), new JsonSerde<>(Tup.class)))
        .toStream()
        .peek((key, value) -> log.info("Page {} delta: {}", key, value))
        .filter((key, tup) -> tup.delta != 0 && tup.previous - tup.delta>0) // avoid the first emission
        .mapValues((key, value) -> value.delta)
        .to("page-view-delta", Produced.with(String(), Long()));

    return streamsBuilder.build();
  }

  record Tup(long previous, long delta) {}
}
