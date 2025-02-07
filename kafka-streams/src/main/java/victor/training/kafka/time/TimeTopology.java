package victor.training.kafka.time;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;

import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class TimeTopology {
  public static void topology(StreamsBuilder streamsBuilder) {


    streamsBuilder.stream("time-input", Consumed.with(String(), String()))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5).minusMillis(100)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek((key, value) -> log.info("Page {} viewed {} times", key, value))
        .selectKey((key, value) -> key.key())
        .to("time-tumbling", Produced.with(String(), String()));

  }
}
