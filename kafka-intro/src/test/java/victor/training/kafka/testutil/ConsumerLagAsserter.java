package victor.training.kafka.testutil;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerLagAsserter {

  private final Environment env;
  private final KafkaListenerEndpointRegistry kafkaListeners;

  /**
   * Verifies there is no pending (unconsumed) message in any topic in the broker.
   * Fails with AssertionError and a detailed report when lag is detected.
   */
  public void assertNoUnconsumedMessagesInBroker() {
    String bootstrap = Objects.requireNonNull(env.getProperty("spring.kafka.bootstrap-servers"),
        "spring.kafka.bootstrap-servers property required");
    try (Admin admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
      // Limitarea verificării DOAR la topicele la care există listeneri activi acum
      Set<String> allTopics = admin.listTopics().names().get(2, TimeUnit.SECONDS);
      Set<String> listenerTopics = resolveListenerTopics(admin);
      // Intersectăm cele două seturi pentru siguranță
      Set<String> topicsToCheck = allTopics.stream()
          .filter(listenerTopics::contains)
          .collect(Collectors.toSet());
      if (topicsToCheck.isEmpty()) {
        log.info("No active listener topics detected. Nothing to verify in broker.");
        return;
      }
      verifyNoLag(admin, topicsToCheck);
    } catch (Exception e) {
      throw new AssertionError("Failed to verify unconsumed messages in broker", e);
    }
  }

  /**
   * Verifies there is no pending (unconsumed) message in the topics listened by Spring Kafka listener containers.
   */
  public void assertNoUnconsumedMessagesInListenerTopics() {
    String bootstrap = Objects.requireNonNull(env.getProperty("spring.kafka.bootstrap-servers"),
        "spring.kafka.bootstrap-servers property required");
    try (Admin admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
      Set<String> topicsToCheck = resolveListenerTopics(admin);
      if (topicsToCheck.isEmpty()) {
        log.info("No listener topics resolved. Nothing to verify.");
        return;
      }
      verifyNoLag(admin, topicsToCheck);
    } catch (Exception e) {
      throw new AssertionError("Failed to verify unconsumed messages in listener topics", e);
    }
  }

  private Set<String> resolveListenerTopics(Admin admin) throws ExecutionException, InterruptedException {
    Set<String> allTopics = admin.listTopics().names().get();
    Set<String> topics = new HashSet<>();
    for (MessageListenerContainer c : kafkaListeners.getListenerContainers()) {
      var props = c.getContainerProperties();
      String[] t = props.getTopics();
      if (t != null && t.length > 0) {
        topics.addAll(Arrays.asList(t));
      }
      Pattern pat = props.getTopicPattern();
      if (pat != null) {
        topics.addAll(allTopics.stream().filter(s -> pat.matcher(s).matches()).collect(Collectors.toSet()));
      }
      var tps = props.getTopicPartitions();
      if (tps != null && tps.length > 0) {
        Arrays.stream(tps).forEach(tp -> topics.add(tp.getTopicPartition().topic()));
      }
    }
    return topics;
  }

  private void verifyNoLag(Admin admin, Set<String> topics) throws Exception {
    if (topics.isEmpty()) return;

    // Describe topics and partitions
    Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames()
        .get(2, TimeUnit.SECONDS);
    List<TopicPartition> partitions = descriptions.values().stream()
        .flatMap(d -> d.partitions().stream().map(p -> new TopicPartition(d.name(), p.partition())))
        .toList();
    if (partitions.isEmpty()) return;

    // Latest offsets
    Map<TopicPartition, OffsetSpec> request = partitions.stream()
        .collect(toMap(tp -> tp, tp -> OffsetSpec.latest()));
    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = admin.listOffsets(request)
        .all().get(2, TimeUnit.SECONDS);

    // All ACTIVE groups (with at least one member)
    List<String> allGroupIds = admin.listConsumerGroups().all().get(2, TimeUnit.SECONDS)
        .stream().map(ConsumerGroupListing::groupId).toList();
    Map<String, ConsumerGroupDescription> groupDescriptions = admin.describeConsumerGroups(allGroupIds)
        .all().get(2, TimeUnit.SECONDS);
    List<String> activeGroupIds = groupDescriptions.entrySet().stream()
        .filter(e -> !e.getValue().members().isEmpty())
        .map(Map.Entry::getKey)
        .toList();

    // Build lag report
    Map<String, List<String>> errorsByTopic = new TreeMap<>();

    // Track partitions that have at least one committed offset by any group
    Set<TopicPartition> withCommitted = new HashSet<>();
    // Track topics that have at least one committed offset (ie. have/ had subscribers)
    Set<String> topicsWithAnyCommitted = new HashSet<>();

    for (String groupId : activeGroupIds) {
      Map<TopicPartition, OffsetAndMetadata> committed;
      try {
        committed = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get(2, TimeUnit.SECONDS);
      } catch (Exception e) {
        // Skip transient issues, continue with other groups
        log.warn("Could not fetch offsets for group {}: {}", groupId, e.getClass().getSimpleName());
        continue;
      }
      Map<TopicPartition, OffsetAndMetadata> relevant = committed.entrySet().stream()
          .filter(e -> topics.contains(e.getKey().topic()))
          .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
      withCommitted.addAll(relevant.keySet());
      topicsWithAnyCommitted.addAll(relevant.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet()));
      for (var entry : relevant.entrySet()) {
        TopicPartition tp = entry.getKey();
        long end = Optional.ofNullable(endOffsets.get(tp)).map(ListOffsetsResult.ListOffsetsResultInfo::offset).orElse(0L);
        long committedOffset = entry.getValue().offset();
        long lag = Math.max(end - committedOffset, 0);
        if (lag > 0) {
          errorsByTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>())
              .add(String.format("%s [group=%s] lag=%d (end=%d, committed=%d)",
                  tp, groupId, lag, end, committedOffset));
        }
      }
    }

    // Partitions that have data but no group committed anything are also errors,
    // BUT ignore topics that are not subscribed by any consumer group at all
    for (var e : endOffsets.entrySet()) {
      TopicPartition tp = e.getKey();
      if (!topics.contains(tp.topic())) continue;
      long end = e.getValue().offset();
      if (end > 0 && !withCommitted.contains(tp) && topicsWithAnyCommitted.contains(tp.topic())) {
        errorsByTopic.computeIfAbsent(tp.topic(), k -> new ArrayList<>())
            .add(String.format("%s has %d records produced but no consumer group committed offsets", tp, end));
      }
    }

    if (!errorsByTopic.isEmpty()) {
      StringBuilder sb = new StringBuilder("Unconsumed Kafka messages detected:\n");
      errorsByTopic.forEach((topic, lines) -> {
        sb.append("  ").append(topic).append(':').append('\n');
        lines.forEach(line -> sb.append("    - ").append(line).append('\n'));
      });
      throw new AssertionError(sb.toString());
    }
  }
}
