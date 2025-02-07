package victor.training.kafka.win;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class WinTopology {

  public static void sketch(StreamsBuilder builder) {
    // String uId -> Long win
    KStream<String, Long> winStream = builder.stream("win");

    winStream
        .groupByKey()
        .count() // String uId -> Long numberOfWins
        .toStream()
        .to("wins-count");

    winStream
        .groupByKey()
        .reduce(Long::sum)
        .toStream()
        .to("win-amount");

    winStream
        .groupByKey()
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))// tubling de 5s
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)).advanceBy(Duration.ofMillis(100)) // hopping
        .aggregate(() -> new RunningAgg(0, 0), (k, v, agg) -> agg.combine(v))
        .toStream()
        .to("win-stats");
  }
  public static void topology(StreamsBuilder builder) {

    // 1) Hoping Window: eg: sa detectezi nr de castiguri > 10 in 5 minute, evaluate la fiecare secunda
    // la castiguri > 1000E/min => chemi baetii

    // 2) Tumbling Window de 5s: ferestre nesuprapuse consecutive: eg aduni metrici ca-n graphana: calculand "running-average"
    // la pierderi > 1000K /sapt => + 50E

    // 3) Session Window de 30m: in sesiunea de dimineata a tocat 1000E, seara doar 300E
    //.     cand mai vine un mesaj cu aceeasi cheie, se prelungeste cu +30m fereastra curenta ca poate mai vine ceva
    //.  la 20 de lossuri intr-un session sa-i dai un bonus

    // 4) Sliding Window ~ Hopping cu step0.00001 care emite eventuri doar cand are valori noi
    // ~1 use-case
  }

  record RunningAgg(long max, long total){
    public RunningAgg combine(Long win) {
      return new RunningAgg(Math.max(max, win), total + win);
    }
  }
}


////
//stream("win") // String uId -> Long win
//.groupByKey()
//
//.count() //1) numar cate winuri a avut uId
//
//.reduce((v1,v1) -> v1.amount + v2.amount) // 2) sum up wins=><String,Long>
//
//    .aggregate((k,v,agg)->agg) // 3) aggregate eg: sa calculezi max(win), totalWin =><String,WinStats>
