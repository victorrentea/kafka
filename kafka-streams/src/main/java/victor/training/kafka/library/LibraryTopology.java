package victor.training.kafka.library;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import victor.training.kafka.KafkaUtils;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;


@Slf4j
public class LibraryTopology {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaUtils.createTopic("checkout-initiated");

    Topology topology = topology();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

    kafkaStreams.start();

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }

  public static Topology topology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    
    // Split the checkout-initiated event into individual book checkouts
    streamsBuilder.stream("checkout-initiated", Consumed.with(Serdes.String(), LibrarySerdes.serdeFor(LibraryEvent.CheckoutInitiated.class)))
        .peek((key, value) -> log.warn("Splitting: {}/{}", key, value))
        .flatMapValues(checkout -> checkout.checkouts().stream()
            .map(book -> new LibraryEvent.CheckoutBook(book.bookId(), checkout.userId(), checkout.checkoutId()))
            .toList())
        .selectKey((key, value) -> value.bookId())
        .peek((key, value) -> log.warn("Splitted: {}/{}", key, value))
        .to("checkout-book", Produced.with(Serdes.String(), LibrarySerdes.serdeFor(LibraryEvent.CheckoutBook.class)));

    // checkout each book
    streamsBuilder.stream("checkout-book", Consumed.with(Serdes.String(), LibrarySerdes.serdeFor(LibraryEvent.CheckoutBook.class)))
        .peek((key, value) -> log.warn("Book>Checking out: {}/{}", key, value))
        .mapValues(value -> new LibraryEvent.BookCheckedOut(value.bookId(), value.checkoutId(), true))
        .peek((key, value) -> log.warn("Book>Checkout result: {}/{}", key, value))
        .to("book-checked-out", Produced.with(Serdes.String(), LibrarySerdes.serdeFor(LibraryEvent.BookCheckedOut.class)));

    streamsBuilder.stream("book-checked-out", Consumed.with(Serdes.String(), LibrarySerdes.serdeFor(LibraryEvent.BookCheckedOut.class)))
        .peek((key, value) -> log.warn("IN: {}/{}", key, value))
        .map((key, value) -> KeyValue.pair(value.checkoutId(), value.success()))
        .groupByKey(Grouped.with(Serdes.UUID(), Serdes.Boolean()))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1)))
        .aggregate(() -> true,
            (UUID key, Boolean a, Boolean b) -> b && a,
            Materialized.with(Serdes.UUID(), Serdes.Boolean())
        )
        .toStream((windowedKey,value)->windowedKey.key())
        .to("checkout-result", Produced.with(Serdes.UUID(), Serdes.Boolean()));

    Topology topology = streamsBuilder.build();
    System.out.println(topology.describe());
    return topology;
  }
}
