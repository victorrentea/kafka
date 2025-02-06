package victor.training.kafka.words;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import victor.training.kafka.KafkaUtils;

import java.util.Properties;

@Slf4j
public class WordsMain {

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaUtils.createTopic(WordsTopology.WORDS_TOPIC);
    KafkaUtils.createTopic(WordsTopology.WORD_COUNT_TOPIC);
    StreamsBuilder streams = new StreamsBuilder();
    WordsTopology.createTopology(streams);
    KafkaStreams kafkaStreams = new KafkaStreams(streams.build(), properties);

    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }

}
