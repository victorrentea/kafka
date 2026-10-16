package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.With;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
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
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toMap;
import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words"; // <uuid>:<text>
  public static final String WORD_COUNT_TOPIC = "word-count"; // <word>:<occurences>
  public static final String WORD_COUNT_TABLE = "word-count-table"; // state store name
  public static final String DICTIONARY_TOPIC = "dictionary"; // <word0>:<word1>

  public static void createTopology(StreamsBuilder streamsBuilder) {
    KTable<String, String> dictionaryTable =
            streamsBuilder.table(DICTIONARY_TOPIC, Consumed.with(String(), String()));
    // halo:hi, halo:hello

    var ks = streamsBuilder.stream(WORDS_TOPIC, Consumed.with(String(), String()))
            // :hello World, :halo
            // TODO split messages into words
            .flatMapValues( text -> Arrays.stream(text.split("\\s+")).map(String::toLowerCase).toList())
            // :hello, :world, :halo
            .map((k, v) -> KeyValue.pair(v, v))
            // hello:hello, world:world, halo:halo

            // requires kafka record and ktable row to have the same key
            .leftJoin(dictionaryTable,(streamValue, kTableValue)->
                    kTableValue != null ? kTableValue : streamValue) // in your case: oldRecord.set(moreDataFromRecord).thenoutut enriched record
            // hello:hello, world:world, halo:hello
            .map((k, v) -> KeyValue.pair(v, 1L));
            // hello:1, world:1, hello:1
        // + more ks.groupByKey()
        ks.groupByKey()
            .aggregate( //seed:
                () -> new Agg(0L, LocalDateTime.now()),
                // how to fold:
                (key, value, aggregate) -> new Agg(aggregate.total + value, LocalDateTime.now()),
                // storage:
                Materialized.<String, Agg, KeyValueStore<Bytes, byte[]>>as(WORD_COUNT_TABLE)
                    .withKeySerde(String())
                    .withValueSerde(new JsonSerde<>(Agg.class))
            )
            .toStream() // k:v only gets out when changed
            // hello:Agg{1..}, world:Agg{1..}, hello:Agg{2..}

            .mapValues(agg -> agg.total)

            // hello:1, world:1, hello:2


    // TODO lowercase

    // TODO (last) normalize via dictionary

    // TODO count occurrences

    // TODO 1 .count
    // TODO 2 .aggregate (generic)

    // hello:1, world:1
    ;

    System.out.println(streamsBuilder.build().describe());
  }

  @With
  record Agg(Long total, LocalDateTime timeReceived) {
  }

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
      Long count = kTable.get(word).total;
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
