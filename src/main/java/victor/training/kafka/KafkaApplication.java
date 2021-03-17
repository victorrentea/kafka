package victor.training.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Arrays.asList;

interface AnalyticsBinding {
   String PAGE_VIEWS_OUT = "pvout";
   String PAGE_VIEWS_IN = "pvin";
   String PAGE_COUNT_MV = "pvmv";
   String PAGE_COUNT_OUT = "pcout";
   String PAGE_COUNT_IN = "pcin";


   @Output(PAGE_VIEWS_OUT)
   MessageChannel pageViewsOut();

   @Input(PAGE_VIEWS_IN)
   KStream<String, PageViewEvent> pageViewsIn();

   @Output(PAGE_COUNT_OUT)
   KStream<String, Long> pageCountOut();

   @Input(PAGE_COUNT_IN)
   KTable<String, Long> pageCountIn();
}

@Slf4j
@Component
class PageViewEventSink {
   @StreamListener(AnalyticsBinding.PAGE_VIEWS_IN)
   @SendTo(AnalyticsBinding.PAGE_COUNT_OUT)
   public KStream<String, String>  process(KStream<String, PageViewEvent> events) {

      log.info("Handling");
//      events.foreach(new ForeachAction<String, PageViewEvent>() {
//         @Override
//         public void apply(String key, PageViewEvent value) {
//            log.info("Received " + key + " : " + value);
//         }
//      });
      return events.filter((key, value) -> value.getDuration() > 10)
          .peek(new ForeachAction<String, PageViewEvent>() {
             @Override
             public void apply(String key, PageViewEvent value) {
                log.info("Aggregating key {}: {}", key, value);
             }
          })
          .map((key, value) -> new KeyValue<>(value.getPage(), "0"))
          .groupByKey(Grouped.with(new StringSerde(), new StringSerde()))
//          .windowedBy(TimeWindows.of(Duration.of(1, ChronoUnit.MINUTES)))
//          .count()
          .count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV))
          .mapValues(l -> l + "")
          .toStream();

//          .toStream();

//         windowedKTable.toStream().foreach((key, value) -> System.out.println(key.key() + ": " + value));

   }
}

@Slf4j
@Component
class PageCountSink {
//   @Bean
//   public Consumer<KTable<String, String>> printer() {
//      return counts ->  counts.toStream()
//          .foreach((key, value) -> log.info(key + " = " + value));
//   }

   @StreamListener(AnalyticsBinding.PAGE_COUNT_IN)
   public void pro(KTable<String, String> counts) {
      counts.toStream()
          .foreach((key, value) -> log.info(key + " = " + value));
   }
}

@Slf4j
@SpringBootApplication
@RestController
@EnableBinding(AnalyticsBinding.class)
public class KafkaApplication implements CommandLineRunner {

   private final MessageChannel pvout;
   private final InteractiveQueryService queryService;

   public KafkaApplication(AnalyticsBinding binding, InteractiveQueryService queryService) {
      this.pvout = binding.pageViewsOut();
      this.queryService = queryService;

   }


   @Bean
   public NewTopic topic1() {
      return new NewTopic("PageViewEventSink-process-applicationId-pvmv-repartition", 10, (short) 1);

      //foo: topic name
      //10: number of partitions
      //2: replication factor
   }

//	EmitterProcessor<String> processor = EmitterProcessor.create();
//
//	@Bean
//	public Supplier<Flux<String>> supplier() {
//		return () -> this.processor;
//	}

   public static void main(String[] args) {
      SpringApplication.run(KafkaApplication.class, args);
   }

   @GetMapping
   public Map<String,Long> hello() {
//		processor.onNext(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
//      return "Halo";
//      queryService.
      Map<String, Long> map = new HashMap<>();
      ReadOnlyKeyValueStore<String, Long> store = queryService.getQueryableStore(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.keyValueStore());

      store.all().forEachRemaining(kv -> map.put(kv.key, kv.value));

      return map;
   }

   @Override
   public void run(String... args) throws Exception {
      List<String> names = asList("user1", "user2", "user3", "user4");
      List<String> pages = asList("page1", "page2", "page3", "page4");

      Runnable r = () -> {
         String rPage = pages.get(new Random().nextInt(pages.size()));
         String rName = names.get(new Random().nextInt(names.size()));

         PageViewEvent event = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);

         Message<PageViewEvent> message = MessageBuilder
             .withPayload(event)
             .setHeader(KafkaHeaders.MESSAGE_KEY, event.getUserId().getBytes(StandardCharsets.UTF_8))
             .build();

         try {
            pvout.send(message);
            log.info("debug " + message);
         } catch (Exception e) {
            log.error(e.getMessage(), e);
         }
      };
      Executors.newScheduledThreadPool(1).scheduleAtFixedRate(r, 1, 1, TimeUnit.SECONDS);
   }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class PageViewEvent {
   private String userId, page;
   private long duration;
}