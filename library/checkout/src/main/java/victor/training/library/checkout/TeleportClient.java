package victor.training.library.checkout;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

@Component
public class TeleportClient {
    private final RestClient restClient;

    public TeleportClient(@Value("${teleport.service.url}") String baseUrl) {
        this.restClient = RestClient.create(baseUrl);
    }

    record TeleportRequest(long userId, List<Long> bookIds) {}

    public void teleport(long userId, List<Long> bookIds) {
        restClient.post()
                .uri("/teleport")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new TeleportRequest(userId, bookIds))
                .retrieve()
                .toBodilessEntity();
    }
}
