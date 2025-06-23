package victor.training.kafka.save.offsets;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singletonList;
import static java.util.stream.IntStream.range;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

import jakarta.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import victor.training.kafka.save.offsets.offsets.ConsumerOffset;
import victor.training.kafka.save.offsets.offsets.ConsumerOffsetRepository;
import victor.training.kafka.offer.Offer;
import victor.training.kafka.offer.OfferRepo;

@Slf4j
@Component
@RequiredArgsConstructor
@Profile("save-offsets")
public class PromotionConsumer {

    public static final double POST_PROCESSING_FAILURE_PROBABILITY = 0.3;
    public static final int NR_OF_PARTITIONS = 1;
    public static final String TOPIC = "create.promotion.command";


    private final OfferRepo offers;

    private final ConsumerOffsetRepository consumerOffsets;

    private final TransactionTemplate transaction;

    private final KafkaProperties kafkaProperties;

    @PostConstruct
    public void startListening() {
        range(0, NR_OF_PARTITIONS)
            .mapToObj(partition -> new TopicPartition(TOPIC, partition))
            .forEach(topicPartition ->
                Thread.ofPlatform().daemon().start(() ->
                    startCustomListener(topicPartition, this::onMessageReceived)
                ));
    }

    private void onMessageReceived(ConsumerOffset newMessageOffset, String payload) {
        transaction.executeWithoutResult(__ -> {
            Offer offer = offers.save(
                new Offer().name(payload));
            log.info("Saved to DB new promo: {}", offer);

            consumerOffsets.save(newMessageOffset);
            log.info("Saved to DB offset: {}", newMessageOffset);
        });

        // simulate post processing failure
        if (ThreadLocalRandom.current().nextDouble(0.0, 1.0) < POST_PROCESSING_FAILURE_PROBABILITY) {
            throw new RuntimeException("Ooupss...!! Simulating failure to acknowledge message: " + payload);
        }
    }

    private void startCustomListener(TopicPartition topicPartition, BiConsumer<ConsumerOffset, String> eventHandler) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(singletonList(topicPartition));
        log.info("Starting custom listener for: {}", topicPartition);

        while (true) {
            ConsumerOffset offset = consumerOffsets.find(topicPartition);
            offset.increment();
            consumer.seek(topicPartition, offset.offsetIndex());

            consumer.poll(ofSeconds(1)).forEach(record -> {
                log.info("Received record: value={}, offset={}", record.value(), record.offset());
                try {
                    eventHandler.accept(offset, record.value());
                } catch (Exception e) {
                    log.error("Error processing record: {}", record, e);
                    // Optionally handle the error, e.g., retry logic, DLQ... etc.
                }
            });
        }
    }

}
