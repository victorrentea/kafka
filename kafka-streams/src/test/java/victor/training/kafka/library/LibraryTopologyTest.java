package victor.training.kafka.library;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import victor.training.kafka.library.LibraryEvent.BookCheckedOut;
import victor.training.kafka.library.LibraryEvent.CheckoutBook;
import victor.training.kafka.library.LibraryEvent.CheckoutInitiated;
import victor.training.kafka.library.LibraryEvent.CheckoutInitiated.BookCheckout;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static java.time.LocalDate.now;
import static org.apache.kafka.common.serialization.Serdes.String;
import static org.assertj.core.api.Assertions.assertThat;

public class LibraryTopologyTest {
  public static final String USER = "jdoe";
  public static final String USER_2 = "smith";
  public static final String BOOK_1 = "BOOK1";
  public static final String BOOK_2 = "BOOK2";
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, CheckoutInitiated> inputTopic;
  private UUID CHECKOUT_ID = UUID.randomUUID();
  private UUID CHECKOUT_ID_2 = UUID.randomUUID();

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    testDriver = new TopologyTestDriver(LibraryTopology.topology(), props);
    inputTopic = testDriver.createInputTopic("checkout-initiated", String().serializer(), LibrarySerdes.serdeFor(CheckoutInitiated.class).serializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void explore() throws InterruptedException {
    inputTopic.pipeInput("key", CheckoutInitiated.builder()
        .checkoutId(CHECKOUT_ID)
        .userId(USER)
        .checkedOutAt(LocalDateTime.now())
        .teleportMethod("priority")
        .checkouts(List.of(
            new BookCheckout(BOOK_1, now().plusDays(7)),
            new BookCheckout(BOOK_2, now().plusDays(7))
        ))
        .build());
    inputTopic.pipeInput("key", CheckoutInitiated.builder()
        .checkoutId(CHECKOUT_ID_2)
        .userId(USER_2)
        .checkedOutAt(LocalDateTime.now())
        .teleportMethod("priority")
        .checkouts(List.of(new BookCheckout(BOOK_1, now().plusDays(7))))
        .build());

    assertThat(testDriver.createOutputTopic("checkout-book", String().deserializer(), LibrarySerdes.serdeFor(CheckoutBook.class).deserializer())
        .readKeyValuesToList()).describedAs("Messages correctly splitted")
        .containsExactlyInAnyOrder(
            new KeyValue<>(BOOK_1, new CheckoutBook(BOOK_1, USER, CHECKOUT_ID)),
            new KeyValue<>(BOOK_2, new CheckoutBook(BOOK_2, USER, CHECKOUT_ID)),
            new KeyValue<>(BOOK_1, new CheckoutBook(BOOK_1, USER_2, CHECKOUT_ID_2))
        );

    assertThat(testDriver.createOutputTopic("book-checked-out", String().deserializer(), LibrarySerdes.serdeFor(BookCheckedOut.class).deserializer())
        .readKeyValuesToList()).describedAs("Individual Books checked out")
        .containsExactlyInAnyOrder(
            new KeyValue<>(BOOK_1, new BookCheckedOut(BOOK_1, CHECKOUT_ID, true)),
            new KeyValue<>(BOOK_2, new BookCheckedOut(BOOK_2, CHECKOUT_ID, true)),
            new KeyValue<>(BOOK_1, new BookCheckedOut(BOOK_1, CHECKOUT_ID_2, true))
        );

    testDriver.advanceWallClockTime(Duration.ofSeconds(2));

//    Thread.sleep(2000);

    assertThat(testDriver.createOutputTopic("checkout-result", Serdes.UUID().deserializer(), Serdes.Boolean().deserializer())
        .readKeyValuesToList()).describedAs("Checkout result")
        .contains(
            new KeyValue<>(CHECKOUT_ID, true),
            new KeyValue<>(CHECKOUT_ID_2, true)
        )
//        .hasSize(2)//NOTE duplicates
    ;

  }
}
