package victor.training.library.e2e;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class CheckoutE2ETest {
    static final HttpClient HTTP = HttpClient.newHttpClient();
    static final String CHECKOUT = "http://localhost:8085";
    static final String MEMBER   = "http://localhost:8082";
    static final String TELEPORT = "http://localhost:8083";
    static final AtomicLong NEXT_USER = new AtomicLong(System.currentTimeMillis());

    @Test
    void happyPath() throws Exception {
        long userId = NEXT_USER.incrementAndGet();
        assertThat(checkout(userId, 101, 102).statusCode()).isEqualTo(200);
    }

    @Test
    void teleporterFails_compensation_allowsRetry() throws Exception {
        long userId = NEXT_USER.incrementAndGet();

        post(TELEPORT + "/teleport/toggle-fail", "");
        assertThat(checkout(userId, 101).statusCode()).isEqualTo(500);

        // compensation removed the books — checking out 5 books must succeed
        assertThat(checkout(userId, 1, 2, 3, 4, 5).statusCode()).isEqualTo(200);
    }

    @Test
    void memberFull_checkoutFails() throws Exception {
        long userId = NEXT_USER.incrementAndGet();
        // unique checkoutId — member's idempotency check is keyed only by (checkoutId, bookId)
        post(MEMBER + "/members/" + userId + "/books",
                """
                {"checkoutId":"pre-fill-%d","bookIds":[1,2,3,4,5]}
                """.formatted(userId));

        assertThat(checkout(userId, 6).statusCode()).isEqualTo(500);
    }

    private HttpResponse<String> checkout(long userId, long... bookIds) throws Exception {
        return post(CHECKOUT + "/checkout",
                """
                {"userId":%d,"bookIds":%s}
                """.formatted(userId, Arrays.toString(bookIds)));
    }

    static HttpResponse<String> post(String url, String body) throws Exception {
        var publisher = body.isBlank()
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
        var req = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(publisher)
                .build();
        return HTTP.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
