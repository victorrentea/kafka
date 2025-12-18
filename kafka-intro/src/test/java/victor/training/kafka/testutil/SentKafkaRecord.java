package victor.training.kafka.testutil;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.extension.*;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Annotation for a JUnit 5 test parameter that requests injection of a
 * {@code CompletableFuture<ConsumerRecord<K,V>>} which will be completed with the first
 * message received from the given topic.
 *
 * Uses the Spring configuration (a {@code ConsumerFactory}) from the test context.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(SentKafkaRecord.Extension.class)
public @interface SentKafkaRecord {
  /**
   * The topic to read the message from.
   * Aliased with {@link #topic()} to allow the concise syntax: {@code @ReceivedKafkaRecord("topic")}.
   */
  @AliasFor("topic")
  String value() default "";

  /** The topic to read the message from. Alias for {@link #value()}. */
  @AliasFor("value")
  String topic() default "";

  /** Prefix for the randomly generated per-test {@code group.id}. */
  String groupIdPrefix() default "test-";

  /**
   * JUnit 5 extension that creates a consumer and completes the future with the first received record.
   */
  class Extension implements ParameterResolver, AfterEachCallback {

    private static class Resources implements ExtensionContext.Store.CloseableResource {
      Consumer<?, ?> consumer;
      Thread poller;
      AtomicBoolean running = new AtomicBoolean(false);

      @Override
      public void close() {
        running.set(false);
        if (consumer != null) {
          try { consumer.wakeup(); } catch (Exception ignored) {}
          try { consumer.close(Duration.ofSeconds(1)); } catch (Exception ignored) {}
        }
        if (poller != null) {
          try { poller.join(1000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
      }
    }

    private static ExtensionContext.Namespace ns(ExtensionContext context) {
      return ExtensionContext.Namespace.create(Extension.class, context.getUniqueId());
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
      boolean annotated = parameterContext.isAnnotated(SentKafkaRecord.class);
      if (!annotated) return false;
      var type = parameterContext.getParameter().getType();
      return CompletableFuture.class.isAssignableFrom(type);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context) {
      SentKafkaRecord ann = parameterContext.findAnnotation(SentKafkaRecord.class).orElseThrow();
      String topic = Optional.ofNullable(ann.value()).filter(s -> !s.isBlank()).orElse(ann.topic());

      var appCtx = SpringExtension.getApplicationContext(context);
      ConsumerFactory<Object, Object> cf = (ConsumerFactory) appCtx.getBean(ConsumerFactory.class);

      // Random group id + 'earliest' as a fallback when there are no commits
      String groupId = ann.groupIdPrefix() + UUID.randomUUID();
      Properties overrides = new Properties();
      overrides.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      Consumer<Object, Object> consumer = cf.createConsumer(groupId, null, null, overrides);

      // Resource management via Store (automatically closed at the end of the test)
      ExtensionContext.Store store = context.getStore(ns(context));
      Resources res = new Resources();
      res.consumer = consumer;
      store.put(Resources.class.getName(), res);

      // Future that will be completed with the first received record
      CompletableFuture<ConsumerRecord<?, ?>> future = new CompletableFuture<>();

      // Subscribe and ensure a quick assignment
      consumer.subscribe(List.of(topic));
      long start = System.currentTimeMillis();
      while (consumer.assignment().isEmpty() && System.currentTimeMillis() - start < 3000) {
        consumer.poll(Duration.ofMillis(100));
      }

      // Start a polling thread that completes the future on the first record
      res.running.set(true);
      res.poller = new Thread(() -> {
        try {
          while (res.running.get() && !future.isDone()) {
            var records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<?, ?> rec : records) {
              future.complete(rec);
              res.running.set(false);
              break;
            }
          }
        } catch (Exception e) {
          // if the test stops the consumer during cleanup, ignore wakeup/close errors
          if (!future.isDone()) future.completeExceptionally(e);
        }
      }, "kafka-test-poller");
      res.poller.setDaemon(true);
      res.poller.start();

      return future;
    }

    @Override
    public void afterEach(ExtensionContext context) {
      ExtensionContext.Store store = context.getStore(ns(context));
      Resources res = store.remove(Resources.class.getName(), Resources.class);
      if (res != null) {
        res.close();
      }
    }
  }
}
