package victor.training.kafka.words;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import victor.training.kafka.KafkaUtils;

import java.util.List;
import java.util.Properties;

public class WordsTopology {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0"); // disable caching for faster outcome
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaUtils.createTopic("word-input");
    KafkaStreams kafkaStreams = new KafkaStreams(topology(), properties);

    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }

  // split by space ["a b", "c"] -> ["a", "b", "c"]
  public static Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("word-input", Consumed.with(Serdes.String(), Serdes.String())) // citesc
        .mapValues(v -> v.toLowerCase())
//        .map((k,v) -> KeyValue.pair(k,v.toLowerCase())) // evita
        //.filter((k, v) -> !"hello".equals(v))
        .flatMapValues(v -> List.of(v.split("\\s+")))
        .peek((k, v) -> System.out.println("record: " + k + "-" + v))
        .to("out-topic", Produced.with(Serdes.String(), Serdes.String())); // scriu

    builder.stream("out-topic", Consumed.with(Serdes.String(), Serdes.String()))
        .groupBy((k,v)->v, Grouped.with(Serdes.String(), Serdes.String())) // repartitionare
        .count() // de cate ori apare key KTable=tabela persistata in Kafka
        .toStream() // emite doar la update + bufferizat
        .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

    return builder.build();
  }

}

// TODO sa ai teste si pe un caz mai simplu de warmup