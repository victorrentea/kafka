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

    private static void resetOffsetsForAllGroups(String bootstrap, List<String> topics) {
      try (Admin admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
        // Describe topics and partitions
        Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames()
            .get(1, TimeUnit.SECONDS);
        List<TopicPartition> partitions = descriptions.values().stream()
            .flatMap(d -> d.partitions().stream()
                .map(tpi -> new TopicPartition(d.name(), tpi.partition())))
            .toList();
        if (partitions.isEmpty()) {
          log.info("No partitions found for topics {}. Nothing to reset.", topics);
          return;
        }
        // Get latest offsets for all partitions
        Map<TopicPartition, OffsetSpec> offsetRequest = partitions.stream()
            .collect(toMap(Function.identity(), tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = admin.listOffsets(offsetRequest).all()
            .get(1, TimeUnit.SECONDS);

        // List all consumer groups
        List<String> allGroupIds = admin.listConsumerGroups().all()
            .get(1, TimeUnit.SECONDS)
            .stream().map(ConsumerGroupListing::groupId)
            .toList();

        for (String groupId : allGroupIds) {
          try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata()
                .get(1, TimeUnit.SECONDS);

            // Filter only partitions from the provided topics where this group has committed offsets
            Map<TopicPartition, OffsetAndMetadata> relevant = committed.entrySet().stream()
                .filter(e -> topics.contains(e.getKey().topic()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (relevant.isEmpty()) {
              continue; // this group didn't consume from these topics
            }
            Map<TopicPartition, OffsetAndMetadata> newOffsets = relevant.keySet().stream()
                .filter(endOffsets::containsKey)
                .collect(toMap(
                    Function.identity(),
                    tp -> new OffsetAndMetadata(endOffsets.get(tp).offset())
                ));
            if (!newOffsets.isEmpty()) {
              log.info("Resetting offsets for group '{}' on {} partitions: {}", groupId, newOffsets.size(), newOffsets);
              admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get(1, TimeUnit.SECONDS);
            }
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.kafka.common.errors.UnknownMemberIdException
                || cause instanceof org.apache.kafka.common.errors.CoordinatorNotAvailableException
                || cause instanceof org.apache.kafka.common.errors.RebalanceInProgressException) {
              log.warn("Skipping offset reset for group '{}' due to transient coordinator state: {}", groupId, cause.toString());
              continue;
            }
            throw new RuntimeException("Resetting failed for groupId=" + groupId, e);
          } catch (InterruptedException | TimeoutException e) {
            throw new RuntimeException("Resetting failed for groupId=" + groupId, e);
          }
        }
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        throw new RuntimeException(e);
      }
    }

  }
}
