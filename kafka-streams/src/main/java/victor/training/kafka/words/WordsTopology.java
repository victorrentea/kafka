package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import victor.training.kafka.KafkaUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.serialization.Serdes.Long;
import static org.apache.kafka.common.serialization.Serdes.String;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsTopology {
  public static final String WORDS_TOPIC = "words";
  public static final String WORD_COUNT_TOPIC = "word-count";
  public static final String WORD_COUNT_TABLE = "word-count-table";

//  @Transactional
//  @KafkaListener
//  public void method(HandoverEvent handover) {
//    for(playerId : handover.tablePlayers) {
//      kafkaTemplate.send("topic",""+playerId, new HandoverPerPlayerEvent(..));
//    }
//  }



  public static void createTopology(StreamsBuilder streamsBuilder) {
    streamsBuilder.<String, String>stream(WORDS_TOPIC, Consumed.with(String(), String()))
        // k, "Hello hello world"
        // k, "hello"
        .flatMapValues((k,v)->Arrays.stream(v.split(" "))
            .map(String::toLowerCase)
            .toList())
        // k, "hello"
        // k, "hello"
        // k, "hello"
        // k, "world"
        .groupBy((k,v)-> v, Grouped.with(String(), String())) // creeaza +1 topic
//        .count() // => KTable
        .aggregate(() -> 0L, new Aggregator<String, String, Long>() {
          @Override
          public Long apply(String key, String value, Long old) {
            return old+1;
          }
        }, Materialized.with(String(), Long()))
        .toStream() // => KStream<String,Long> cu un mesaj pt fiecare update al KTable de mai sus
        .peek((k,v)->log.info("Received message: {} {}", k, v))
        .to(WORD_COUNT_TOPIC, Produced.with(String(), Long()));
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
