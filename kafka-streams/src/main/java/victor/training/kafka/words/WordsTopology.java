package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import java.util.Map;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words";
  public static final String WORD_COUNT_TOPIC = "word-count";
  public static final String WORD_COUNT_TABLE = "word-count-table";

  public static void createTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.<String, String>stream(WORDS_TOPIC, Consumed.with(String(), String()))
        .flatMapValues((key, value) -> Arrays.asList(value.toLowerCase().split(" ")))
        .groupBy((key, value) -> value, Grouped.with(String(), String()))
        .count()
        .toStream()
        .peek((k, v) -> log.info("Word '{}' appears {} times", k, v))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), Long()));

    streamsBuilder
        .stream(WORD_COUNT_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
        .toTable(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_TABLE)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.Long()));
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
    return counts.get(word);
  }

  private final ProducerFactory<String, String> producerFactory;
  @GetMapping("/words")
  public void send(@RequestParam(defaultValue = "Hello world") String m) {
    // the value serializer is special for this topic
    Map<String, Object> configOverrides = Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    KafkaTemplate<String, String> kafkaTemplate2 = new KafkaTemplate<>(producerFactory, configOverrides);
    kafkaTemplate2.send(WORDS_TOPIC, "a", m);
  }
}
