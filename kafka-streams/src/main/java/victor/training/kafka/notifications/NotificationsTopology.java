package victor.training.kafka.notifications;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class NotificationsTopology {
  public static void main(String[] args) { // vanilla Java (no Spring)
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "notifications");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    Properties adminProperties = new Properties();
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    try (var admin = AdminClient.create(adminProperties)) {
      NewTopic userTopic = new NewTopic("user-updated", 1, (short) 1)
          .configs(Map.of(
              "cleanup.policy", "compact",
              "retention.ms", "-1"
          ));
      NewTopic notificationTopic = new NewTopic("notification", 1, (short) 1);
      NewTopic broadcastTopic = new NewTopic("broadcast", 1, (short) 1);

      admin.createTopics(List.of(userTopic, notificationTopic, broadcastTopic));
    }

    KafkaStreams kafkaStreams = new KafkaStreams(topology(), properties);

    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }

  public record UserEmail(String username, String email) {
  }

  public static Topology topology() {
    // aici puteai si sa dai GET user-service/user/{username} < nu e event driven
    // e mult mai safe (replicat) si mai rapid (partitionat) asa.
    // intr-o lume utopica/distopica in care respiram Kafka, REST a murit.
    // din DBul user-service iese un paraias (stream) publicat de ei constient sau furat de noi cu Kafka Connect

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KTable<String, UserUpdated> kTable = streamsBuilder.stream("user-updated", Consumed.with(Serdes.String(), new JsonSerde<>(UserUpdated.class)))

        //.filter((k, userUpdated) -> userUpdated.acceptsEmailNotifications())

//        .mapValues(userUpdated -> new UserEmail(userUpdated.username(), userUpdated.email()))

        .toTable(Materialized.with(Serdes.String(), new JsonSerde<>(UserUpdated.class)));

    streamsBuilder.stream("notification", Consumed.with(Serdes.String(), new JsonSerde<>(Notification.class)))

        .selectKey((k, notification) -> notification.recipientUsername()) // cauzeaza un write/read in remote broker

        .leftJoin(kTable, (notification, user) -> { // TODO extrage o functie de aici
          if (user == null) {
            log.error("Unknown user: {}", notification.recipientUsername());
            return null;
          }
          if (!user.acceptsEmailNotifications()) {
            return null;
          }
          return new SendEmail(notification.message(), user.email());
        })

        .filter((k,v) -> v!=null)

        .to("send-email", Produced.with(Serdes.String(), new JsonSerde<>(SendEmail.class)));

    streamsBuilder.stream("broadcast", Consumed.with(Serdes.String(), new JsonSerde<>(Broadcast.class)))

        .flatMapValues(broadcast -> broadcast.recipientUsernames().stream()
            .map(username -> new Notification(broadcast.message(), username))
            .toList())

        .to("notification", Produced.with(Serdes.String(), new JsonSerde<>(Notification.class)));

    return streamsBuilder.build();
  }
}
