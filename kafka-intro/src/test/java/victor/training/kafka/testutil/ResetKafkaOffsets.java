package victor.training.kafka.testutil;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.lang.annotation.Retention;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.toMap;

@Retention(RUNTIME)
@ExtendWith(ResetKafkaOffsets.Extension.class)
public @interface ResetKafkaOffsets {
  String[] value();
  long waitMillis() default 1000;

  @Slf4j
  class Extension implements BeforeEachCallback {
    @Override
    public void beforeEach(ExtensionContext context) throws InterruptedException {
      var ctx = SpringExtension.getApplicationContext(context);
      var env = ctx.getEnvironment();
      ResetKafkaOffsets annotation = AnnotationUtils.findAnnotation(context.getRequiredTestClass(), ResetKafkaOffsets.class);

      var bootstrapServers = env.getProperty("spring.kafka.bootstrap-servers");

      log.info("Sleeping {}ms before reset offsets...", annotation.waitMillis());
      Thread.sleep(annotation.waitMillis());
      resetOffsetsForAllGroups(bootstrapServers, List.of(annotation.value()));
    }

    private static void resetOffset(String bootstrap, List<String> topics, String groupId) {
      try (Admin admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
        Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames()
            .get(1, TimeUnit.SECONDS);
        List<TopicPartition> partitions = descriptions.values().stream()
            .flatMap(d -> d.partitions().stream()
                .map(tpi -> new TopicPartition(d.name(), tpi.partition())))
            .toList();
        Map<TopicPartition, OffsetSpec> offsetRequest = partitions.stream()
            .collect(toMap(Function.identity(), tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = admin.listOffsets(offsetRequest).all()
            .get(1, TimeUnit.SECONDS);
        Map<TopicPartition, OffsetAndMetadata> newOffsets = endOffsets.entrySet().stream()
            .collect(toMap(
                Map.Entry::getKey,
                e -> new OffsetAndMetadata(e.getValue().offset() - 1)
            ));

        admin.alterConsumerGroupOffsets(groupId , newOffsets).all()
            .get(1, TimeUnit.SECONDS);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
