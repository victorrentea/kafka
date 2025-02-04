package victor.training.kafka.notifications;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.kafka.support.serializer.JsonSerde;
import victor.training.kafka.KafkaUtils;

import java.util.Properties;

@Slf4j
public class NotificationsTopology {
  public static void main(String[] args) { // vanilla Java (no Spring)
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "notifications");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0"); // disable caching for faster outcome
    properties.put("internal.leave.group.on.close", "true"); // faster restart as per https://dzone.com/articles/kafka-streams-tips-on-how-to-decrease-rebalancing

    KafkaStreams kafkaStreams = new KafkaStreams(topology(), properties);

    KafkaUtils.createTopic("notification");
    KafkaUtils.createTopic("user-updated");
    KafkaUtils.createTopic("broadcast");
    kafkaStreams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Runs on control-c
  }

  public static Topology topology() {
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    KTable<String, UserUpdated> userKTable = streamsBuilder.stream("user-updated", Consumed.with(Serdes.String(), new JsonSerde<>(UserUpdated.class)))
        .filter((key, value) -> value.acceptsEmailNotifications())
        .toTable(Materialized.with(Serdes.String(), new JsonSerde<>(UserUpdated.class)));

    streamsBuilder.stream("notification", Consumed.with(Serdes.String(), new JsonSerde<>(Notification.class)))
        .selectKey((key, value) -> value.recipientUsername())
        .repartition(Repartitioned.with(Serdes.String(), new JsonSerde<>(Notification.class)))
        .leftJoin(userKTable, (notification, userUpdated) -> {
          if (userUpdated == null) {
            log.warn("Unknown user: {}", notification.recipientUsername());
            return null;
          }
          return new SendEmail(notification.message(), userUpdated.email());
        })
        .filter((key, value) -> value != null)
        .to("send-email", Produced.with(Serdes.String(), new JsonSerde<>(SendEmail.class)));

    streamsBuilder.stream("broadcast", Consumed.with(Serdes.String(), new JsonSerde<>(Broadcast.class)))
        .flatMap((k,broadcast) -> broadcast.recipientUsernames().stream()
            .map(username -> KeyValue.pair(username, broadcast.message())).toList())
        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
        .join(userKTable, (message, userUpdated) -> new SendEmail(message, userUpdated.email()))
        .to("send-email", Produced.with(Serdes.String(), new JsonSerde<>(SendEmail.class)));

    return streamsBuilder.build();
  }
}
