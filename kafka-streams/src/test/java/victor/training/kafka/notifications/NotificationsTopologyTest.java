package victor.training.kafka.notifications;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import victor.training.kafka.util.CaptureSystemOutput;
import victor.training.kafka.util.CaptureSystemOutput.OutputCapture;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.MethodName.class)
@SuppressWarnings("resource")
public class NotificationsTopologyTest {
  public static final String EMAIL = "jdoe@example.com";
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Notification> notificationInputTopic;
  private TestInputTopic<String, UserUpdated> userUpdatedInputTopic;
  private TestOutputTopic<String, SendEmail> outputTopic;

  @BeforeEach
  final void before() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    Topology topology = NotificationsTopology.topology();
    System.out.println(topology.describe());
    testDriver = new TopologyTestDriver(topology, props);
    notificationInputTopic = testDriver.createInputTopic("notification", Serdes.String().serializer(), new JsonSerde<>(Notification.class).serializer());
    userUpdatedInputTopic = testDriver.createInputTopic("user-updated", Serdes.String().serializer(), new JsonSerde<>(UserUpdated.class).serializer());
    outputTopic = testDriver.createOutputTopic("send-email", Serdes.String().deserializer(), new JsonSerde<>(SendEmail.class).deserializer());
  }

  @AfterEach
  final void after() {
    testDriver.close();
  }

  @Test
  void p1_bootstrap() { // TODO to fix this test, you are allowed to hard-code the "dummy" in production code 😉. KISS.
    userUpdatedInputTopic.pipeInput("jdoe", new UserUpdated("jdoe", "dummy", true));
    notificationInputTopic.pipeInput(new Notification("Hello", "jdoe"));
    sendDummyNotification();
    // builder.stream("..", with(..))
    //.    .mapValue(->.."dummy"..)
    //     .to("..", with(..))

    assertThat(outputTopic.readValuesToList()).containsExactly(new SendEmail("Hello", "dummy"));
  }

  @Test
  void p2_sendsToEmailFromUserUpdated() {
    userUpdatedInputTopic.pipeInput("jdoe", new UserUpdated("jdoe", EMAIL, true));
    notificationInputTopic.pipeInput(new Notification("Hello", "jdoe"));
    sendDummyNotification();;
    // kStream.selectKey(->email).repartition(with...)
    // var kTable = kStream.toTable(with...)
    // kStream.join(kTable, (streamValue, tableValue) -> ...)

    assertThat(outputTopic.readValuesToList()).containsExactly(new SendEmail("Hello", EMAIL));
  }

  @Test
  void p3_doesntSend_whenUnknownUser() {
    notificationInputTopic.pipeInput(new Notification("Hello", "jdoe"));

    assertThat(outputTopic.readValuesToList()).isEmpty();
  }

  @Test
  void p4_doesntSend_whenUserOptedOut() {
    userUpdatedInputTopic.pipeInput("jdoe", new UserUpdated("jdoe", EMAIL, false));
    notificationInputTopic.pipeInput(new Notification("Hello", "jdoe"));
    // var kTable = .stream.filter.toTable

    assertThat(outputTopic.readValuesToList()).isEmpty();
  }

  @Test
  void p5_broadcast() {
    userUpdatedInputTopic.pipeInput("u1", new UserUpdated("u1", "u1@example.com", true));
    userUpdatedInputTopic.pipeInput("u2", new UserUpdated("u2", "u2@example.com", true));
    var broadcastInputTopic = testDriver.createInputTopic("broadcast", Serdes.String().serializer(), new JsonSerde<>(Broadcast.class).serializer());
    broadcastInputTopic.pipeInput(new Broadcast("Broadcast message", List.of("u1", "u2")));

    assertThat(outputTopic.readValuesToList()).containsExactlyInAnyOrder(
        new SendEmail("Broadcast message", "u1@example.com"),
        new SendEmail("Broadcast message", "u2@example.com"));
  }

  @Test
  void p6_doesnt_broadcast_toOptOuts() {
    userUpdatedInputTopic.pipeInput("u1", new UserUpdated("u1", "u1@example.com", true));
    userUpdatedInputTopic.pipeInput("u2", new UserUpdated("u2", "u2@example.com", false));
    var broadcastInputTopic = testDriver.createInputTopic("broadcast", Serdes.String().serializer(), new JsonSerde<>(Broadcast.class).serializer());
    broadcastInputTopic.pipeInput(new Broadcast("Broadcast message", List.of("u1", "u2")));

    assertThat(outputTopic.readValuesToList()).containsExactlyInAnyOrder(
        new SendEmail("Broadcast message", "u1@example.com"));
  }

  @Test
//  @Disabled("⭐️Only for the brave")
  @CaptureSystemOutput
  void p7_logs_whenUnknownUser(OutputCapture outputCapture) {
    notificationInputTopic.pipeInput(new Notification("Hello", "jdoe"));
    sendDummyNotification();
    // kStream.leftJoin(kTable, (streamValue, tableValue) -> if.. + log).filter(

    assertThat(outputCapture.toString()).contains("Unknown user: jdoe");
  }

  @Test
//  @Disabled("⭐️Challenge #2")
  void p99_out_of_order_notifications_within_1_second() throws InterruptedException {
    userUpdatedInputTopic.pipeInput("jdoe", new UserUpdated("jdoe", EMAIL, true));
    // TODO debate: how can this happen in real life?
    notificationInputTopic.pipeInput(new Notification("#2", "jdoe"), Instant.now().plusMillis(500));
    notificationInputTopic.pipeInput(new Notification("#1", "jdoe"), Instant.now());
    // send dummy event to update stream time
    sendDummyNotification();
    assertThat(outputTopic.readValuesToList()).containsExactly(
        new SendEmail("#1", EMAIL),
        new SendEmail("#2", EMAIL));
  }

  @Test
//  @Disabled("⭐️Challenge #3")
  void p99_eliminates_duplicated_notifications_within_1_second() throws InterruptedException {
    userUpdatedInputTopic.pipeInput("jdoe", new UserUpdated("jdoe", EMAIL, true));
    // TODO how can this happen in real life
    notificationInputTopic.pipeInput(new Notification("#1", "jdoe"), Instant.now().plusMillis(500));
    notificationInputTopic.pipeInput(new Notification("#1", "jdoe"), Instant.now());
    notificationInputTopic.pipeInput(new Notification("#2", "jdoe"), Instant.now());
    // send dummy event to update stream time
    sendDummyNotification();
    assertThat(outputTopic.readValuesToList()).containsExactly(
        new SendEmail("#1", EMAIL),
        new SendEmail("#2", EMAIL)
        );
  }

  private void sendDummyNotification() {
    notificationInputTopic.pipeInput(new Notification("DUMMY", "DUMMY"), Instant.now().plus(1, ChronoUnit.DAYS));
  }
}
