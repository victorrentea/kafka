package victor.training.kafka.words;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

import java.util.*;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words"; // <- "a", "b", "a"
  public static final String WORD_COUNT_TOPIC = "word-count"; // ->...
  public static final String WORD_COUNT_TABLE = "word-count-table";

  // to test: http://localhost:8080/words
  // to test: http://localhost:8080/words?m=Craciun
  public static void createTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.stream(WORDS_TOPIC, Consumed.with(String(), String()))
        // {,"a"}, {,"b C"}, {,"a"}
        .flatMapValues(phrase -> List.of(phrase.split("\\s+"))) // Kafka e mai rapid daca NU schimbi cheia
        // {,"a"}, {,"b"}, {,"C"}, {,"a"}
        .mapValues(v -> v.toLowerCase())
        // {,"a"}, {,"b"}, {,"c"}, {,"a"}
        .groupBy((k, v) -> v, Grouped.with(String(), String()))
        // topic ascuns creat
        //.count()
//        .aggregate(() -> 0L,(k,v,old)->old+1, Materialized.with(String(),Long()))
        .aggregate(() -> new Agg(0, 0), (k, v, old) -> {
              System.out.println("AGREG");
              return new Agg(old.count() + 1, old.sum() + k.length());
            }
            , Materialized.with(String(), new JsonSerde<>(Agg.class)))

        .toStream()
        // emite mesaj pt orice modificare in KTable parinte

        // {"a",1}, {"b",1},{"c",1}, {"a",2}
        .peek((k, v) -> log.info("Got " + k + ": " + v))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), new JsonSerde<>(Agg.class))); // .doOnNext (reactor)
    System.out.println(streamsBuilder.build().describe());
  }

  @Data
  static final class Agg {
    private final int count;
    private final int sum;

    Agg(int count, int sum) {
      this.count = count;
      this.sum = sum;
    }

    public int count() {
      return count;
    }

    public int sum() {
      return sum;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj == null || obj.getClass() != this.getClass()) return false;
      var that = (Agg) obj;
      return this.count == that.count &&
             this.sum == that.sum;
    }

    @Override
    public int hashCode() {
      return Objects.hash(count, sum);
    }

    @Override
    public String toString() {
      return "Agg[" +
             "count=" + count + ", " +
             "sum=" + sum + ']';
    }
  }

  @Autowired
  void configureTopology(StreamsBuilder streamsBuilder) {
    KafkaUtils.createTopic(WORDS_TOPIC);
    KafkaUtils.createTopic(WORD_COUNT_TOPIC);
    createTopology(streamsBuilder);
  }

  private final StreamsBuilderFactoryBean factoryBean;
  @GetMapping("/words-count/{word}")
  public Long getWordCounts(@PathVariable String word) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(WORD_COUNT_TABLE, QueryableStoreTypes.keyValueStore())
    );
    return counts.get(word); // ~Map<Key,Value>
  }

  private final ProducerFactory<String, String> producerFactory;
  @GetMapping("/words") // http://localhost:8080/words
  public void send(@RequestParam(defaultValue = "Hello\t world") String m) {
    // the value serializer is special for this topic
    Map<String, Object> configOverrides = Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    KafkaTemplate<String, String> kafkaTemplate2 = new KafkaTemplate<>(producerFactory, configOverrides);
    kafkaTemplate2.send(WORDS_TOPIC, m);
  }
}
