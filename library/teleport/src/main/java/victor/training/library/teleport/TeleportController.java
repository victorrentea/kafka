package victor.training.library.teleport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@RestController
public class TeleportController {
    private final AtomicBoolean failNext = new AtomicBoolean(false);

    record TeleportRequest(long userId, List<Long> bookIds) {}

    @PostMapping("/teleport")
    public void teleport(@RequestBody TeleportRequest request) {
        if (failNext.getAndSet(false)) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Teleporter malfunction! Books remain in warehouse.");
        }
        log.info("Teleporting books {} to user {}'s living room", request.bookIds(), request.userId());
    }

    @PostMapping("/teleport/toggle-fail")
    public String toggleFail() {
        boolean nowFailing = !failNext.get();
        failNext.set(nowFailing);
        return "Teleporter will " + (nowFailing ? "FAIL" : "succeed") + " on next call";
    }

    void resetFailNext() {
        failNext.set(false);
    }
}
