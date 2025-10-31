//package victor.training.kafka.words;
//
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.processor.api.Record;
//import org.springframework.kafka.support.serializer.JsonSerde;
//import reactor.util.function.Tuples;
//import victor.training.kafka.notifications.events.UserUpdated;
//
//import java.time.Duration;
//
//import static java.time.Duration.*;
//import static org.apache.kafka.common.serialization.Serdes.String;
//
//public class Alarms {
//  record CDR(String imsi, int traffic) {}
//  public static void method(StreamsBuilder streamsBuilder) {
//    KTable<String, String> imsiToFleet = streamsBuilder.table("imsi-to-fleet");
//    // imsi,fleetId
//    record HourlyTraffic(String fleetId, Window timeWindow, int traffic) {}
//
//    streamsBuilder.stream("raw-traffic-per-imsi")
//        // KStream<imsi,traffic>
//        .join(imsiToFleet, (traffic, fleetId) -> new Record<>(fleetId, traffic))
//        // join KTable<imsi, fleetId>
//        // KStream<fleetId, traffic>
//        .groupByKey()
//        .windowedBy(TimeWindows.ofSizeAndGrace(ofHours(1), ofSeconds(5)).advanceBy(ofMinutes(5)))
//        .aggregate(() -> 0, (fleetId, traffic, oldTraffic) -> oldTraffic + traffic)
//
//        //#musthave nu emite mesaje de agregare intermdiare, ci doar la finalul ferestrei
//        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
////        .suppress(Suppressed.untilTimeLimit(Suppressed.BufferConfig.unbounded()))
//        .toStream()
//    // agrega datele pe o fereastra de 1h la fiecare 5min
//        .mapValues((windowedKey, traffic) ->
////            "FleetId " + windowedKey.key() +
////            " pe time " + windowedKey.window() +
////            " a inregistrat traffic " + traffic
//            new HourlyTraffic(windowedKey.key(), windowedKey.window(), traffic)
//        )
//        .selectKey((windowedKey, value) -> windowedKey.key() /*fleetId*/)
//        .join(alarmsTable,(hourlyTraffic, alarmsPerFleet)->traffic+alarme)
//        .filter((trafic+alarme)-> traffic > alarma)
//    .mapValue(traffic+alarm->new AlarmToSend)
//        .to("alarm-topic")
//    ;
//         windowedKey.window() = {1:00-2:00},{1:05-2:05},
//        // KStream<fleetId, traffic-per-ora>
//
//
//
//    // join KTable<fleetId, threshold>
//    // KStream<fleetId, {traffic-per-ora + threshold}>
//    // .filter(if (traffic-per-ora > threshold))
//    // send to alarm-topic KStream<fleetId, String message>
//
//  }
//}
//    // follow up: daca aveam KTable<fleetId, Fleet{List<imsi>}>
