package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.internals.InMemoryWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

@Slf4j
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreams {
  //  Serde<Event> event1Serde = Serdes.serdeFrom(new Serializer(), new Deserializer());
//  @Autowired
//  void buildPipeline(StreamsBuilder streamsBuilder) {
//    KStream<String, String> messageStream = streamsBuilder
//        .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE));
//
//    KTable<String, Long> wordCounts = messageStream
//        .mapValues((ValueMapper<String, String>) String::toLowerCase)
//        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
//        .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
//        .count();
//
//    wordCounts.toStream().to("output-topic");
//  }
  private final StreamsBuilder builder;

  @Autowired
  public void onAppStarted(StreamsBuilder streamsBuilder) {
    log.warn("⭐️⭐️⭐ Starting KStreams ⭐️⭐️⭐️");
    // ~= // @KafkaListener(topics = "myTopic") public void consume(String message) {log.info(message);}


    // C) StockUpdated{productId, int stock}
    // A) OutOfStock{productId} + BackInStock{productId} = what catalog-service needs
// convert C to a with a KStream

//    streamsBuilder.stream("stock-updated", Consumed.with(Serdes.String(), EventSerdes.serdeFor(StockUpdated.class)))
//            .filter((key, value) -> value.stock() == 0)
//            .map((key, value) -> KeyValue.pair(value.productId(), new OutOfStock(value.productId())))
//            .to("out-of-stock", Produced.with(Serdes.String(), EventSerdes.serdeFor(OutOfStock.class)));

    streamsBuilder.stream("myTopic", Consumed.with(Serdes.String(), EventSerdes.serdeFor(Event.class)))
        .foreach((key, value) -> log.warn("Received key: " + key + " Value: " + value));

    streamsBuilder.stream("checkout", Consumed.with(Serdes.String(), EventSerdes.serdeFor(CheckoutBooks.class)))
        .peek((key, value) -> log.warn("Splitting: {}/{}", key, value))
        .flatMapValues(checkout -> checkout.checkouts().stream()
            .map(book -> new CheckoutBook(book.bookId(), checkout.userId(), checkout.checkoutId()))
            .toList())
        .map((key, value) -> KeyValue.pair(value.bookId(), value))
        .peek((key, value) -> log.warn("Splitted: {}/{}", key, value))
        .to("checkout-books", Produced.with(Serdes.String(), EventSerdes.serdeFor(CheckoutBook.class)));

    streamsBuilder.stream("checkout-books", Consumed.with(Serdes.String(), EventSerdes.serdeFor(CheckoutBook.class)))
        .peek((key, value) -> log.warn("Book>Checking out: {}/{}", key, value))
        .mapValues(value -> new BookCheckedOutEvent(value.bookId(), value.checkoutId(), Math.random() > 0.5))
        .peek((key, value) -> log.warn("Book>Checkout result: {}/{}", key, value))
        .to("book-checked-out", Produced.with(Serdes.String(), EventSerdes.serdeFor(BookCheckedOutEvent.class)));

    streamsBuilder.stream("book-checked-out", Consumed.with(Serdes.String(), EventSerdes.serdeFor(BookCheckedOutEvent.class)))
        .peek((key, value) -> log.warn("IN: {}/{}", key, value))
        .map((key, value) -> KeyValue.pair(value.checkoutId(), value.success()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Boolean()))
        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(2)))
//        .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(2),Duration.ofSeconds(1)))// 5 sec from the first message with that key
//        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(2)))// 5 sec from the first message with that key

//        .aggregate(() -> true,
//            (String key, Boolean value, Boolean aggregate) -> aggregate && value,
//            Materialized.with(Serdes.String(), Serdes.Boolean())
//        )
        .count()
        .suppress(untilWindowCloses(unbounded()))
//        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(1),unbounded()))
        .toStream()
        .peek((key, value) -> log.warn("OUTw: {}/{}", key, value))
        .map((key, value) -> KeyValue.pair(key.key(), value))

//        .peek((key, value) -> log.warn("OUT: {}/{}", key, value))
//        .to("checkout-result", Produced.with(Serdes.String(), Serdes.Boolean()));
        .foreach((k,v) -> log.warn("OUT: {}/{}", k, v));

    log.warn("✅✅✅Started KStreams");
  }

//  @Bean
//  public Function<KStream<String,BookCheckedOutEvent>, KStream<String, BooksCheckedOutEvent>> mergeBookCheckout() {
//    return input -> input.selectKey((key, value) -> value.checkoutId().toString())
//        .map((key, value) -> KeyValue.pair(key, value.success()))
//        .peek((key, value) -> log.info("Merging: {}", value))
//        .groupByKey()
//        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(1000)))
//        .aggregate(() -> true, (String key, Boolean value, Boolean b) -> b && value,
//            Materialized.<String, Boolean, WindowStore<Bytes, byte[]>>as("checkout-aggregates")
//                .withKeySerde(Serdes.String())
//                .withValueSerde(Serdes.Boolean())
////              .suppress(untilWindowCloses(unbounded()))
//        )
//        .toStream()
//        .map((key, value) -> KeyValue.pair(key.key(), new BooksCheckedOutEvent(UUID.fromString(key.key()), value)))
//        .peek((key, value) -> log.info("Merged: {}", value));
//  }


  // KStream<String, String> input = builder.stream("event-topic");
  //input.groupByKey().aggregate(
  //    () -> "",
  //    EventProcessor::apply,
  //    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("eventshot")
  //        .withCachingDisabled()
  //)
  //.toStream().to("eventshot-topic");
}
