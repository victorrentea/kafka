package victor.training.kafka.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Slf4j
@Component
@RequiredArgsConstructor
class LoanRequestedListener {
    static final String TOPIC = "loan-requested";
    private final LoanSchemaAdapter schemaAdapter;
    private final LoanService loanService;

    @KafkaListener(topics = TOPIC, containerFactory = "schemaStringContainerFactory")
    void onMessage(String rawJson) throws JsonProcessingException {
        loanService.process(schemaAdapter.convert(rawJson));
    }
}

@Slf4j
@Component
class LoanService {
    void process(LoanRequestedEvent event) {
        log.info("Loan request: {} {} amount={}", event.firstName(), event.lastName(), event.amount());
    }
}

@Configuration
class SchemaKafkaConfig {
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> schemaStringContainerFactory(KafkaProperties props) {
        var config = new HashMap<>(props.buildConsumerProperties(null));
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        return factory;
    }
}
