package victor.training.kafka.save.offsets;

import static java.util.stream.IntStream.range;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import lombok.SneakyThrows;
import victor.training.kafka.KafkaTest;
import victor.training.kafka.offer.Offer;
import victor.training.kafka.offer.OfferRepo;

@Testcontainers
@ActiveProfiles("save-offsets")
@ComponentScan(basePackages = "victor.training.kafka.save.offsets")
class AtomicallyAckMsgAndUpdateDbTest extends KafkaTest {

    @Container
    static ComposeContainer env = new ComposeContainer(new File("../docker-compose.yml"))
        .withServices("broker")
        .withLocalCompose(true);

    @Autowired
    private OfferRepo offers;

    @Test
    @SneakyThrows
    void atomicallyAckMsg_andUpdateDB_fails() {
        range(0, 100)
            .mapToObj("Offer #%s"::formatted)
            .forEach(msg -> publishMessage(OfferConsumer.TOPIC, msg));

        Thread.sleep(5_000);

        assertThat(offers.findByNameContaining("Offer"))
            .as("proof that this approach will cause duplicates in DB, " +
                "because the message is not ack atomically with the DB update")
            .hasSize(100)
            .map(Offer::name)
            .doesNotHaveDuplicates();
    }

    @Test
    @SneakyThrows
    void atomicallyAckMsg_andUpdateDB_savingOffsetsToDB() {
        range(0, 100)
            .mapToObj("Promo #%s"::formatted)
            .forEach(msg -> publishMessage(PromotionConsumer.TOPIC, msg));

        Thread.sleep(5_000);

        assertThat(offers.findByNameContaining("Promo"))
            .hasSize(100)
            .map(Offer::name)
            .doesNotHaveDuplicates();
    }


    @SneakyThrows
    private void publishMessage(String topic, String offerName) {
        try (var producer = kafkaProducer()) {
            producer.send(new ProducerRecord<>(topic, offerName)).get();
        }
    }

    static KafkaProducer<String, String> kafkaProducer() {
        return new KafkaProducer<>(
            Map.of(
                BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
            ));
    }
}
