package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
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

import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words"; // <- "a", "b", "a"
  public static final String WORD_COUNT_TOPIC = "word-count"; // ->...
  public static final String WORD_COUNT_TABLE = "word-count-table";

  // to test: http://localhost:8080/words
  // to test: http://localhost:8080/words?m=Two%20Words
  public static void createTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.stream(WORDS_TOPIC, Consumed.with(String(), String()))
        // {,"a"}, {,"b C"}, {,"a"}
        .flatMapValues(phrase -> List.of(phrase.split("\\s+"))) // Kafka e mai rapid daca NU schimbi cheia
        // {,"a"}, {,"b"}, {,"C"}, {,"a"}
        .mapValues(v -> v.toLowerCase())
        // {,"a"}, {,"b"}, {,"c"}, {,"a"}
        .groupBy((k, v) -> v, Grouped.with(String(), String()))

        //.count()
        //.aggregate(() -> 0L,(k,v,old)->old+1, Materialized.with(String(),Long()))
        .aggregate(() -> new Agg(0, 0),
            (k, v, old) -> {
              System.out.println("AGREG");
              return new Agg(old.count() + 1, old.sum() + k.length());
            },
            Materialized.<String, Agg, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_TABLE)
                .withKeySerde(String())
                .withValueSerde(new JsonSerde<>(Agg.class)))

        .toStream()
        // emits only if KTable changed

        // {"a",1}, {"b",1},{"c",1}, {"a",2}
        .peek((k, v) -> log.info("Got " + k + ": " + v))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), new JsonSerde<>(Agg.class)));

    System.out.println(streamsBuilder.build().describe());
  }

  public record Agg(int count, int sum) {
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
    ReadOnlyKeyValueStore<String, Agg> counts = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(WORD_COUNT_TABLE, QueryableStoreTypes.keyValueStore())
    );
    Agg agg = counts.get(word);
    return agg == null ? 0L : (long) agg.count();
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
