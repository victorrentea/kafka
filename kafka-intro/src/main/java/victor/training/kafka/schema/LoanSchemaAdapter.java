package victor.training.kafka.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

// Converts any schema version of the loan-requested event to the current v2 structure.
@Component
@RequiredArgsConstructor
class LoanSchemaAdapter {
    private final ObjectMapper objectMapper;

    LoanRequestedEvent convert(String json) throws JsonProcessingException {
        JsonNode node = objectMapper.readTree(json);
        if (node.has("fullName")) return upcastV1(node);         // v1 → v2
        if (node.get("phone") != null && node.get("phone").isArray()) return downcastV3(node); // v3 → v2
        return objectMapper.treeToValue(node, LoanRequestedEvent.class);
    }

    // v1: fullName{firstName,lastName,middleName} + loanAmount, no phone
    private LoanRequestedEvent upcastV1(JsonNode node) {
        JsonNode name = node.get("fullName");
        return new LoanRequestedEvent(
                name.get("firstName").asText(),
                name.get("lastName").asText(),
                node.get("loanAmount").asInt(),
                null);
    }

    // v3: phone became an array — take the first element
    private LoanRequestedEvent downcastV3(JsonNode node) {
        String phone = node.get("phone").isEmpty() ? null : node.get("phone").get(0).asText();
        return new LoanRequestedEvent(
                node.get("firstName").asText(),
                node.get("lastName").asText(),
                node.get("amount").asInt(),
                phone);
    }
}
