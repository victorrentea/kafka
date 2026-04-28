package victor.training.kafka.debezium;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Prerequisites: start root docker-compose (Kafka) then kafka-debezium/docker-compose.yml (Postgres + Debezium Connect).
 */
@SpringBootTest
class DebeziumTest {

    static final BlockingQueue<String> customerMessages = new LinkedBlockingQueue<>();
    static final BlockingQueue<String> addressMessages = new LinkedBlockingQueue<>();
    static final ObjectMapper mapper = new ObjectMapper();

    @TestConfiguration
    static class ListenerConfig {
        @KafkaListener(topics = "dbserver1.public.customers", groupId = "cdc-test-customers")
        void onCustomer(@Payload(required = false) String msg) { if (msg != null) customerMessages.offer(msg); }

        @KafkaListener(topics = "dbserver1.public.shipping_address", groupId = "cdc-test-addresses")
        void onAddress(@Payload(required = false) String msg) { if (msg != null) addressMessages.offer(msg); }
    }

    @BeforeAll
    static void registerConnector() throws Exception {
        var http = HttpClient.newHttpClient();

        // Delete and recreate to ensure table.include.list is up to date
        http.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8083/connectors/inventory-connector"))
                        .DELETE().build(),
                HttpResponse.BodyHandlers.discarding());
        Thread.sleep(2_000);

        String config = """
                {
                  "name": "inventory-connector",
                  "config": {
                    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                    "database.hostname": "postgres",
                    "database.port": "5432",
                    "database.user": "postgres",
                    "database.password": "postgres",
                    "database.dbname": "inventory",
                    "topic.prefix": "dbserver1",
                    "table.include.list": "public.customers,public.shipping_address",
                    "plugin.name": "pgoutput",
                    "snapshot.mode": "never"
                  }
                }
                """;

        var response = http.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:8083/connectors"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(config))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("register connector").isEqualTo(201);

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            var status = http.send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8083/connectors/inventory-connector/status"))
                            .GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (status.body().contains("\"RUNNING\"")) return;
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("Debezium connector did not reach RUNNING state within 30s");
    }

    @Autowired
    JdbcTemplate jdbc;

    private static JsonNode waitForPayload(BlockingQueue<String> queue, String containingText) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            String msg = queue.poll(1, SECONDS);
            if (msg != null && msg.contains(containingText))
                return mapper.readTree(msg).path("payload");
        }
        return null;
    }

    @Test
    void insert_customer_produces_create_event() throws Exception {
        String email = "insert-" + UUID.randomUUID() + "@test.com";
        jdbc.update("INSERT INTO customers(first_name, last_name, email) VALUES (?, ?, ?)", "Joe", "Doe", email);

        JsonNode payload = waitForPayload(customerMessages, email);

        assertThat(payload).isNotNull();
        assertThat(payload.path("op").asText()).isEqualTo("c");
        assertThat(payload.path("before").isNull()).isTrue();
        assertThat(payload.path("after").path("first_name").asText()).isEqualTo("Joe");
        assertThat(payload.path("after").path("email").asText()).isEqualTo(email);
    }

    @Test
    void update_customer_produces_update_event() throws Exception {
        String email = "update-" + UUID.randomUUID() + "@test.com";
        jdbc.update("INSERT INTO customers(first_name, last_name, email) VALUES (?, ?, ?)", "Jane", "Old", email);
        jdbc.update("UPDATE customers SET last_name = ? WHERE email = ?", "New", email);

        JsonNode payload = waitForPayload(customerMessages, email);
        while (payload != null && !"u".equals(payload.path("op").asText())) {
            payload = waitForPayload(customerMessages, email);
        }

        assertThat(payload).isNotNull();
        assertThat(payload.path("op").asText()).isEqualTo("u");
        assertThat(payload.path("before").path("last_name").asText()).isEqualTo("Old");
        assertThat(payload.path("after").path("last_name").asText()).isEqualTo("New");
    }

    @Test
    void delete_customer_produces_delete_event() throws Exception {
        String email = "delete-" + UUID.randomUUID() + "@test.com";
        jdbc.update("INSERT INTO customers(first_name, last_name, email) VALUES (?, ?, ?)", "Del", "Me", email);
        jdbc.update("DELETE FROM customers WHERE email = ?", email);

        JsonNode payload = waitForPayload(customerMessages, email);
        while (payload != null && !"d".equals(payload.path("op").asText())) {
            payload = waitForPayload(customerMessages, email);
        }

        assertThat(payload).isNotNull();
        assertThat(payload.path("op").asText()).isEqualTo("d");
        assertThat(payload.path("after").isNull()).isTrue();
        assertThat(payload.path("before").path("first_name").asText()).isEqualTo("Del");
    }

    @Test
    void insert_customer_and_shipping_address_produces_two_independent_cdc_events() throws Exception {
        // Two separate CDC messages land on two separate topics.
        // PROBLEM (not solved here): correlating parent + child messages requires
        // knowing they came from the same DB transaction. Without Debezium transaction
        // metadata or an Outbox (_OUT) table, there is no built-in correlation key.
        String email = "parent-child-" + UUID.randomUUID() + "@test.com";
        jdbc.update("INSERT INTO customers(first_name, last_name, email) VALUES (?, ?, ?)", "Alice", "Smith", email);
        Integer customerId = jdbc.queryForObject("SELECT id FROM customers WHERE email = ?", Integer.class, email);
        jdbc.update("INSERT INTO shipping_address(customer_id, street, city, zip_code) VALUES (?, ?, ?, ?)",
                customerId, "123 Main St", "Springfield", "12345");

        JsonNode customerPayload = waitForPayload(customerMessages, email);
        assertThat(customerPayload).isNotNull();
        assertThat(customerPayload.path("op").asText()).isEqualTo("c");
        assertThat(customerPayload.path("after").path("first_name").asText()).isEqualTo("Alice");

        JsonNode addressPayload = waitForPayload(addressMessages, String.valueOf(customerId));
        assertThat(addressPayload).isNotNull();
        assertThat(addressPayload.path("op").asText()).isEqualTo("c");
        assertThat(addressPayload.path("after").path("street").asText()).isEqualTo("123 Main St");
        assertThat(addressPayload.path("after").path("customer_id").asInt()).isEqualTo(customerId);
    }
}
