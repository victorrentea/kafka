package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
        .flatMapValues(phrase-> List.of(phrase.split("\\s+"))) // Kafka e mai rapid daca NU schimbi cheia
        // {,"a"}, {,"b"}, {,"C"}, {,"a"}
        .mapValues(v->v.toLowerCase())
        // {,"a"}, {,"b"}, {,"c"}, {,"a"}
        .groupBy((k,v)->v, Grouped.with(String(), String()))
        // {"a",??}, {"b",??},{"c",??}, {"a",??} topic ascuns creat
        //.count()
        .aggregate(() -> 0L,(k,v,old)->old+1, Materialized.with(String(),Long()))
        .toStream()
        // {"a",1}, {"b",1},{"c",1}, {"a",2}
        .peek((k,v)->log.info("Got "+ k + ": " + v))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), Long())); // .doOnNext (reactor)
  }
  record Agg(int count, int sum) {}

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
    return counts.get(word);
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
