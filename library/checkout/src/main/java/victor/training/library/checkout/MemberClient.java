package victor.training.library.checkout;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.List;

@Component
public class MemberClient {
    private final RestClient restClient;

    public MemberClient(@Value("${member.service.url}") String baseUrl) {
        this.restClient = RestClient.create(baseUrl);
    }

    record AddBooksRequest(String checkoutId, List<Long> bookIds) {}

    public void addBooks(long userId, String checkoutId, List<Long> bookIds) {
        restClient.post()
                .uri("/members/{userId}/books", userId)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new AddBooksRequest(checkoutId, bookIds))
                .retrieve()
                .toBodilessEntity();
    }

    public void removeBooks(long userId, String checkoutId) {
        restClient.delete()
                .uri("/members/{userId}/books/{checkoutId}", userId, checkoutId)
                .retrieve()
                .toBodilessEntity();
    }
}
