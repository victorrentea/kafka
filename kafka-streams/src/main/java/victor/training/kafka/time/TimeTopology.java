package victor.training.kafka.time;

import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
public class TimeTopology {

  public static Topology tumbling() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
//        .filter((key, value) -> value != null)
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

  record Alarm(String uId, String message) {
    @Override
    public int hashCode() {
      return uId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      Alarm alarm = (Alarm) o;
      return Objects.equals(uId, alarm.uId);
    }
  }

  //- dupa *5 pierderi consecutive* la *acelasi* joc, primesti $5, max 1bonus/7 zile
  // 0 0 0 0 0 *
  // 0 0 2 0 0
  // 0 0 0 0 0 * 0 0
  record GameResult(long amount, Instant timestamp) {
    private boolean isWin() {
      return amount() != 0;
    }
  }

  public static Topology hopping() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    var winsOver30MinutesWindow = streamsBuilder.stream("time-input", Consumed.with(String(), Long()))
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)).advanceBy(Duration.ofMillis(100)))
        .reduce((a, b) -> a + b)
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .peek(logWindow("Hopping"));

    winsOver30MinutesWindow
        .selectKey((key, value) -> key.key())
        .to("time-hopping", Produced.with(String(), Long()));

    winsOver30MinutesWindow
        .filter((key, value) -> value > 1000)
        .map((window, value) ->
            KeyValue.pair(window.key(), new Alarm(window.key(),
                "ALERT: Sa vina baetii pt ca useru " +
                window.key() + " a castigat " + value +
                " in intervalul  " + window.window().startTime() +
                " - " + window.window().endTime())))
        .selectKey((k, v) -> "baetii")
        .groupByKey(Grouped.with(String(), new JsonSerde<>(Alarm.class)))
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)))
        .aggregate(() -> Set.<Alarm>of(),
            (k, v, set) -> {
              System.out.println("agg: " + set);
              return Stream.concat(set.stream(), Stream.of(v))
                  .collect(Collectors.toSet());
            },
            Materialized.with(String(), new JsonSerde<>(Set.class)))
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
        .toStream()
        .selectKey((k, v) -> k.key())
        .to("win-alerts", Produced.with(String(), new JsonSerde<>(Set.class)));
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
