package victor.training.kafka.schema;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import victor.training.kafka.IntegrationTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.stream.Stream;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static victor.training.kafka.schema.LoanRequestedListener.TOPIC;

public class LoanRequestedListenerTest extends IntegrationTest {

    @Autowired
    KafkaProperties kafkaProperties;

    @MockitoSpyBean
    LoanService loanService;

    KafkaTemplate<String, String> rawTemplate;

    @BeforeEach
    void setupRawTemplate() {
        var config = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        rawTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }

    static Stream<Arguments> versions() {
        return Stream.of(
                Arguments.of("schema/loan-approved-v1.json", new LoanRequestedEvent("John", "Doe", 10000, null)),
                Arguments.of("schema/loan-approved-v2.json", new LoanRequestedEvent("John", "Doe", 10000, "0700123456")),
                Arguments.of("schema/loan-approved-v3.json", new LoanRequestedEvent("John", "Doe", 10000, "0700123456"))
        );
    }

    @ParameterizedTest
    @MethodSource("versions")
    void handlesAllVersions(String jsonFile, LoanRequestedEvent expected) throws Exception {
        rawTemplate.send(TOPIC, loadJson(jsonFile)).get();
        verify(loanService, timeout(5000)).process(expected);
    }

    private String loadJson(String classpathResource) throws IOException {
        try (var stream = getClass().getClassLoader().getResourceAsStream(classpathResource)) {
            return new String(stream.readAllBytes());
        }
    }
}
