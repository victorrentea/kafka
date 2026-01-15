package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

import java.util.*;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words"; // -><word>
  public static final String WORD_COUNT_TOPIC = "word-count"; // <word>-><occurences>
  public static final String WORD_COUNT_TABLE = "word-count-table"; // state store name
  public static final String DICTIONARY_TOPIC = "dictionary"; // <word0>-><word1>

  public static void createTopology(StreamsBuilder streamsBuilder) {
    KTable<String, String> dictionaryTable =
        streamsBuilder.table(DICTIONARY_TOPIC, Consumed.with(String(), String()));
    // ă->a

    streamsBuilder.stream(WORDS_TOPIC, Consumed.with(String(), String()))
        // ?->ă ?->B a
        .mapValues(s -> s.toLowerCase())
        // ?->ă ?->⭐️b a
        .flatMapValues(s -> Arrays.asList(s.split("\\s"))) // split message
        // ?->ă ?->b ?->a⭐️

        .map((k,v)->new KeyValue<>(v,v))
        // ⭐️ă->ă ⭐️b->b ⭐️a->a
        // JOIN with latest data from another source
        .leftJoin(dictionaryTable, (streamValue, tableValue) -> tableValue != null ? tableValue : streamValue)
        // ă->a⭐️ b->b a->a

        .groupBy((k,v)->v)
        // split the events in "logical stream" by what you selected that are then aggregated within that group

//        .count(Materialized.<String, Long, KeyValueStore<Bytes,byte[]>>as(WORD_COUNT_TABLE)
//                .withKeySerde(String())
//                .withValueSerde(Long()))
        // you might need a sum, max, average,
//        new MyAggVal(sum, max, distinct)
        .aggregate(() -> new Agg(0L, 0L),
            (key, value, old) -> new Agg(old.total + 1, old.lengthSum+key.length()),
            Materialized.<String, Agg, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_TABLE)
                .withKeySerde(String())
                .withValueSerde(new JsonSerde<>(Agg.class)))

//        .aggregate(() -> 0L, (key, value, old) -> old + 1,
//            Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_TABLE)
//                .withKeySerde(String())
//                .withValueSerde(Long()))
        // KTable is a storage in Kafka ~ compact topic, but that promises to only store latest value for any key
        .toStream()
        .mapValues(agg->agg.total)
        // will emit an event in the "KStream of changes" any time there's a change in the KTable
        // a->1 b->1 a->2

        .peek(((key, value) -> log.info("Output={}->{}", key, value)))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), Long()))
    ;

    // KStream 1->a 1->b 2->c
    // KStream -> KTable
    // KStream
//    streamsBuilder.stream("topic-a") // r1 r2 r3
//            .toTable() // saved as they arrive, overwritting if matching key
//        .toStream(); // you get the unique changes
    // read project (ktable).get(key)


    System.out.println(streamsBuilder.build().describe());
  }

  record Agg(Long total, Long lengthSum) {}


  // ---- support code ----
  @Autowired
  void configureTopology(StreamsBuilder streamsBuilder) {
    log.info("Creating topology");
    KafkaUtils.createTopic(WORDS_TOPIC);
    KafkaUtils.createTopic(WORD_COUNT_TOPIC);
    KafkaUtils.createTopic(DICTIONARY_TOPIC);
    createTopology(streamsBuilder);
    log.info("Created topology");
  }

  private final StreamsBuilderFactoryBean factoryBean;

  @GetMapping("words/count")
  public Map<String, Long> getWordCounts(@RequestParam(required = false) String word) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, Agg> kTable = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType(WORD_COUNT_TABLE,
            QueryableStoreTypes.keyValueStore())
    );
    if (word != null) {
      Long count = kTable.get(word).total; // ~ read projection
      if (count == null) {
        return Map.of();
      } else {
        return Map.of(word, count);
      }
    } else {
      try (var it = kTable.all()) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
            .collect(toMap(kv -> kv.key, kv -> kv.value.total));
      }
    }
  }

  private final ProducerFactory<String, String> producerFactory;

  @GetMapping("words/send") // http://localhost:8080/words
  public String send(@RequestParam(defaultValue = "Hello\t world") String m) {
    // the value serializer is special for this topic
    Map<String, Object> configOverrides = Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    KafkaTemplate<String, String> kafkaTemplate2 = new KafkaTemplate<>(producerFactory, configOverrides);
    kafkaTemplate2.send(WORDS_TOPIC, m);
    return "✅ sent. see <a href='http://localhost:8080/words/count'>counts</a>";
  }

  // also runnable standalone
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    props.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaUtils.createTopic(WordsTopology.WORDS_TOPIC);
    KafkaUtils.createTopic(WordsTopology.WORD_COUNT_TOPIC);
    StreamsBuilder streams = new StreamsBuilder();
    WordsTopology.createTopology(streams);
    KafkaStreams kafkaStreams = new KafkaStreams(streams.build(), props);

    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }
}
