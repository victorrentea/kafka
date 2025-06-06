package victor.training.kafka.game;

import lombok.With;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.time.Instant;

import static org.apache.kafka.common.serialization.Serdes.String;


public class OffersTopology {
  //- dupa *5 pierderi consecutive* la *acelasi* joc, primesti $5, max 1bonus/7 zile
// 0 0 0 0 0 *
// 0 0 2 0 0
// 0 0 0 0 0 * 0 0
  record GameResult(long amount, Instant timestamp) {
    private boolean isWin() {
      return amount() != 0;
    }
  }

  public static void offer1(StreamsBuilder streamsBuilder) {
    KStream<String, GameResult> winStream = streamsBuilder.stream("win");
    winStream
        .groupByKey()
        .aggregate(() -> Agg1.ZERO, (uId, gameResult, agg) -> agg.apply(gameResult))
        .filter((uId, agg) -> agg.bonusNow())
        .mapValues((uid, agg) -> "Bonus pentru user " + uid)
        .toStream()
        .to("bonus", Produced.with(String(), String()));
  }

  record Agg1(
      @With
      int consecutiveLosses,
      @With
      Instant lastBonusInstant,
      @With
      boolean bonusNow) {
    public static final Agg1 ZERO = new Agg1(0, Instant.now().minus(Duration.ofDays(8)), false);
    public static final int NUMBER_OF_LOSES_FOR_BONUS = 5;
    public static final int MIN_DAYS_BETWEEN_BONUS = 7;

    public Agg1 apply(GameResult newResult) {
      if (newResult.isWin()) return withConsecutiveLosses(0).withBonusNow(false);
      // give a bonus every 7 days AND every 5 lost games to increase and maintain Player addiction
      long daysSinceLastWin = Duration.between(lastBonusInstant, newResult.timestamp()).toDays();
      if (consecutiveLosses + 1 == NUMBER_OF_LOSES_FOR_BONUS && daysSinceLastWin > MIN_DAYS_BETWEEN_BONUS) {
        return withConsecutiveLosses(0).withLastBonusInstant(Instant.now()).withBonusNow(true);
      }
      return withConsecutiveLosses(consecutiveLosses + 1).withBonusNow(false);
    }
  }

  //- dupa 10 zile de joc consecutive primesti $5 > noOfConsecDays, lastPlayDate
  //- daca pierzi la black jack cu 20 iti da compensatie, 1/zi. > lastBonusDate

}
