package victor.training.kafka.words;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
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

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequiredArgsConstructor
public class WordsApi {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final StreamsBuilderFactoryBean factoryBean;

  @Autowired
  void consumerWordCountOutput(StreamsBuilder streamsBuilder) {
    KafkaUtils.createTopic("word-count-output");
    streamsBuilder
        .stream("word-count-output", Consumed.with(Serdes.String(), Serdes.Long()))
        .peek((k, v) -> log.info("Received update: Word '{}' appears {} times", k, v))
        .toTable(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("word-count-table").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()));
//        .toTable(Materialized.as("word-count-table"));
  }
  @GetMapping("/words-count/{word}")
  public Long getWordCounts(@PathVariable String word) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("word-count-table", QueryableStoreTypes.keyValueStore())
    );
    return counts.get(word);
  }
  @GetMapping("/words-count")
  public Map<String,Long> getAllWordCounts( ) {
    KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
    ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams.store(
        StoreQueryParameters.fromNameAndType("word-count-table", QueryableStoreTypes.keyValueStore())
    );
    Map<String, Long> map = new HashMap<>();
    try (KeyValueIterator<String, Long> all = counts.all()) {
      while (all.hasNext()) {
        KeyValue<String, Long> entry = all.next();
        map.put(entry.key, entry.value);
      }
      return map;
    }
  }

  private final ProducerFactory<String, String> producerFactory;
  @GetMapping("/words")
  public void send(@RequestParam(defaultValue = "Hello world") String m) {
    Producer<String, String> producer = new KafkaProducer<>(producerFactory.getConfigurationProperties(), Serdes.String().serializer(), Serdes.String().serializer());
    // have to use a manual producer since I don't want to use the JsonSerializer configured global per spring app
    producer.send(new ProducerRecord<>("word-input", "a", m));
  }


}
