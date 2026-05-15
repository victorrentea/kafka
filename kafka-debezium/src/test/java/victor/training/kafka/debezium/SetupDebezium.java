package victor.training.kafka.debezium;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

public class SetupDebezium {

  public static void registerConnector() throws Exception {
    try (var http = HttpClient.newHttpClient()) {

      // Delete and recreate to ensure table.include.list is up to date
      http.send(HttpRequest.newBuilder()
              .uri(URI.create("http://localhost:8083/connectors/inventory-connector"))
              .DELETE().build(),
          HttpResponse.BodyHandlers.discarding());

      Thread.sleep(2_000);

      String config = Files.readString(Path.of("debezium.connector.json"));

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
    }
    throw new IllegalStateException("Debezium connector did not reach RUNNING state within 30s");
  }
}
