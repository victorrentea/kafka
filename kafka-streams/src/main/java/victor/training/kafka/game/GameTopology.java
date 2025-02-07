package victor.training.kafka.game;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

import static org.apache.kafka.common.serialization.Serdes.*;
import static org.apache.kafka.common.serialization.Serdes.String;

@SuppressWarnings("ALL")
@Slf4j
public class GameTopology {
  public static final String GAME_FINISHED_TOPIC = "game-finished";
  public static final String COMPENSATING_PAYOUT_TOPIC = "compensating-payout";

  // after CL consecutive losses at the same game, you get $C, once every D time
  public static void topology(
      StreamsBuilder streamsBuilder,
      int consecutiveLosses,
      double compensationAmount,
      Duration duration) {


    streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("game-finished-timestamps"),
        String(), Long())
    );


    streamsBuilder.stream(GAME_FINISHED_TOPIC,
            Consumed.with(String(), new JsonSerde<>(GameFinished.class)))
        .groupBy((unused, gameFinished) -> gameFinished.userId(),
            Grouped.with(String(), new JsonSerde<>(GameFinished.class)))
        .aggregate(
            () -> new A(0, null),
            (String key, GameFinished value, A aggregate) ->
                new A(value.isLoss() ? aggregate.consecutiveFailures + 1 : 0, value)
            , Materialized.with(String(), new JsonSerde<>(A.class)))
        .toStream()
        .peek((userId, a) -> log.info("User {} had {} consecutive losses", userId, a.consecutiveFailures))
        .filter((userId, a) -> a.consecutiveFailures >= consecutiveLosses)
        .peek((userId, a) -> log.info("User {} exceeded the number of losses for compensation: {}", userId, consecutiveLosses))
        .processValues(new FixedKeyProcessorSupplier<String, A, A>() {
          @Override
          public FixedKeyProcessor<String, A, A> get() {
            return new FixedKeyProcessor<String, A, A>() {
              private KeyValueStore<String, Long> store;
              private FixedKeyProcessorContext<String, A> context;

              @Override
              public void init(FixedKeyProcessorContext<String, A> context) {
                store = (KeyValueStore<String, Long>) context.getStateStore("game-finished-timestamps");
                this.context=context;
              }

              @Override
              public void process(FixedKeyRecord<String, A> record) {
                Long lastTimestamp = store.get(record.key());
                long thisTimestamp = record.value().gameFinished.timestamp();
                log.info("This record ts: {}, last ts: {}", thisTimestamp, lastTimestamp);
                if (lastTimestamp == null || thisTimestamp - lastTimestamp >= duration.toMillis()) {
                  store.put(record.key(), thisTimestamp); // Update timestamp
                  context.forward(record);
                } else {
                  log.warn("Despite loosing {} times, user {} is not eligible for compensation yet: delta= {}",
                      record.value().consecutiveFailures,
                      record.key(),thisTimestamp-lastTimestamp);
                }
              }
            };
          }
        }, "game-finished-timestamps")
        .peek((userId, a) -> log.info("User {} is eligible for compensation", userId))
        .mapValues(a -> new CompensatingPayout(a.gameFinished.userId(), compensationAmount, a.gameFinished.timestamp()))
        .to(COMPENSATING_PAYOUT_TOPIC, Produced.with(String(), new JsonSerde<>(CompensatingPayout.class)));

  }

  record A(int consecutiveFailures, GameFinished gameFinished) {
  }
}

@Builder
record GameFinished(
    String userId,
    String gameType,
    String roundId,
    double betAmount,
    double payoutAmount,
    long timestamp) {
  public boolean isLoss() {
    return payoutAmount == 0;
  }
}

record CompensatingPayout(
    String userId,
    double amount,
    long timestamp
) {
}