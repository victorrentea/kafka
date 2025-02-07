package victor.training.kafka.time;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class TimeTopology {
  public static Topology tumbling() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek(logWindow("Tumbling"))
        .selectKey((key, value) -> key.key())
        .to("time-tumbling", Produced.with(String(), Long()));
    return streamsBuilder.build();

}
  public static Topology hopping() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)).advanceBy(Duration.ofMillis(100)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek(logWindow("Hopping"))
        .selectKey((key, value) -> key.key())
        .to("time-hopping", Produced.with(String(), Long()));
    return streamsBuilder.build();

  }
  public static Topology sliding() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
        .groupByKey()
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek(logWindow("Sliding"))
        .selectKey((key, value) -> key.key())
        .to("time-sliding", Produced.with(String(), Long()));

    return streamsBuilder.build();
  }
  public static Topology session() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
        .groupByKey()
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek(logWindow("Session"))
        .selectKey((key, value) -> key.key())
        .to("time-session", Produced.with(String(), Long()));

    return streamsBuilder.build();
  }

  private static ForeachAction<? super Windowed<String>, ? super Long> logWindow(String label) {
    return (Windowed<String> key, Long value) -> {
      Instant startInstant = key.window().startTime();
      LocalDateTime startTime = LocalDateTime.ofInstant(startInstant, ZoneId.systemDefault());
      Instant endInstant = key.window().endTime();
      LocalDateTime endTime = LocalDateTime.ofInstant(endInstant, ZoneId.systemDefault());
      System.out.println("Window " + startTime.toLocalTime() + " - " + endTime.toLocalTime() + " = " + value);
    };
  }
}
