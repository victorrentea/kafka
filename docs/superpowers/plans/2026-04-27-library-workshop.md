# Library Workshop Module — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scaffold four Spring Boot microservices (Checkout/Member/Teleport/Reminder) under a `library` parent module, connected via `RestClient`, with Kafka stubs for progressive workshop conversion.

**Architecture:** Checkout (8080) orchestrates Member (8082) + Teleport (8083) via `RestClient`. If Teleport fails, Checkout calls Member's compensation endpoint to undo the book additions. Member enforces max-5 books and idempotency via a `checkoutId` UUID. Reminder (8084) is an empty stub with a commented-out `@KafkaListener`.

**Tech Stack:** Spring Boot 3.5, Spring Web (`RestClient`), Spring Data JPA + H2 (Member only), Lombok, JUnit 5 + MockMvc + Mockito

---

## File Map

```
library/
  pom.xml
  requests.http
  checkout/
    pom.xml
    src/main/java/victor/training/library/checkout/
      CheckoutApplication.java
      CheckoutController.java
      CheckoutRequest.java
      CheckoutService.java
      MemberClient.java
      TeleportClient.java
      Events.java
    src/main/resources/application.properties
    src/test/java/victor/training/library/checkout/
      CheckoutServiceTest.java
  member/
    pom.xml
    src/main/java/victor/training/library/member/
      MemberApplication.java
      MemberBook.java
      MemberBookRepo.java
      MemberController.java
      Events.java
    src/main/resources/application.properties
    src/test/java/victor/training/library/member/
      MemberControllerTest.java
    src/test/resources/application.properties
  teleport/
    pom.xml
    src/main/java/victor/training/library/teleport/
      TeleportApplication.java
      TeleportController.java
      Events.java
    src/main/resources/application.properties
    src/test/java/victor/training/library/teleport/
      TeleportControllerTest.java
    src/test/resources/application.properties
  reminder/
    pom.xml
    src/main/java/victor/training/library/reminder/
      ReminderApplication.java
      Events.java
    src/main/resources/application.properties
```

---

### Task 1: Maven scaffold

**Files:**
- Modify: `pom.xml` (root — add `library` module)
- Create: `library/pom.xml`
- Create: `library/checkout/pom.xml`
- Create: `library/member/pom.xml`
- Create: `library/teleport/pom.xml`
- Create: `library/reminder/pom.xml`

- [ ] **Step 1: Add `library` to root `pom.xml`**

In `/pom.xml`, add `<module>library</module>` to the `<modules>` block:

```xml
<modules>
    <module>kafka-avro</module>
    <module>kafka-intro</module>
    <module>kafka-tx</module>
    <module>kafka-streams</module>
    <module>library</module>
</modules>
```

- [ ] **Step 2: Create `library/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>victor.training</groupId>
        <artifactId>kafka</artifactId>
        <version>1.0</version>
    </parent>
    <artifactId>library</artifactId>
    <packaging>pom</packaging>
    <modules>
        <module>checkout</module>
        <module>member</module>
        <module>teleport</module>
        <module>reminder</module>
    </modules>
</project>
```

- [ ] **Step 3: Create `library/checkout/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>victor.training</groupId>
        <artifactId>library</artifactId>
        <version>1.0</version>
    </parent>
    <artifactId>checkout</artifactId>
</project>
```

- [ ] **Step 4: Create `library/member/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>victor.training</groupId>
        <artifactId>library</artifactId>
        <version>1.0</version>
    </parent>
    <artifactId>member</artifactId>
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-data-jpa</artifactId>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
        </dependency>
    </dependencies>
</project>
```

- [ ] **Step 5: Create `library/teleport/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>victor.training</groupId>
        <artifactId>library</artifactId>
        <version>1.0</version>
    </parent>
    <artifactId>teleport</artifactId>
</project>
```

- [ ] **Step 6: Create `library/reminder/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>victor.training</groupId>
        <artifactId>library</artifactId>
        <version>1.0</version>
    </parent>
    <artifactId>reminder</artifactId>
</project>
```

- [ ] **Step 7: Verify Maven resolves all modules**

```bash
mvn validate -pl library,library/checkout,library/member,library/teleport,library/reminder --no-transfer-progress
```
Expected: `BUILD SUCCESS` for all 5 modules.

- [ ] **Step 8: Commit**

```bash
git add pom.xml library/pom.xml library/checkout/pom.xml library/member/pom.xml library/teleport/pom.xml library/reminder/pom.xml
git commit -m "chore: scaffold library multi-module Maven structure"
```

---

### Task 2: Member service

**Files:**
- Create: `library/member/src/main/java/victor/training/library/member/MemberApplication.java`
- Create: `library/member/src/main/java/victor/training/library/member/MemberBook.java`
- Create: `library/member/src/main/java/victor/training/library/member/MemberBookRepo.java`
- Create: `library/member/src/main/java/victor/training/library/member/MemberController.java`
- Create: `library/member/src/main/java/victor/training/library/member/Events.java`
- Create: `library/member/src/main/resources/application.properties`
- Create: `library/member/src/test/java/victor/training/library/member/MemberControllerTest.java`
- Create: `library/member/src/test/resources/application.properties`

- [ ] **Step 1: Write failing tests for Member endpoints**

Create `library/member/src/test/java/victor/training/library/member/MemberControllerTest.java`:

```java
package victor.training.library.member;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class MemberControllerTest {
    @Autowired MockMvc mockMvc;
    @Autowired MemberBookRepo repo;

    @BeforeEach
    void setUp() {
        repo.deleteAll();
    }

    @Test
    void addsBooksToMember() throws Exception {
        mockMvc.perform(post("/members/1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"checkoutId":"abc123","bookIds":[1,6]}
                                """))
                .andExpect(status().isOk());

        assertThat(repo.countByUserId(1L)).isEqualTo(2);
    }

    @Test
    void idempotent_sameCheckoutId_addsOnce() throws Exception {
        String body = """{"checkoutId":"dup-id","bookIds":[10]}""";
        mockMvc.perform(post("/members/2/books")
                .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk());
        mockMvc.perform(post("/members/2/books")
                .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk());

        assertThat(repo.countByUserId(2L)).isEqualTo(1);
    }

    @Test
    void rejects_whenMemberExceeds5Books() throws Exception {
        mockMvc.perform(post("/members/3/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""{"checkoutId":"over5","bookIds":[1,2,3,4,5,6]}"""))
                .andExpect(status().isConflict());
    }

    @Test
    void compensation_removesAllBooksForCheckoutId() throws Exception {
        mockMvc.perform(post("/members/4/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""{"checkoutId":"to-undo","bookIds":[7,8]}"""))
                .andExpect(status().isOk());

        mockMvc.perform(delete("/members/4/books/to-undo"))
                .andExpect(status().isOk());

        assertThat(repo.countByUserId(4L)).isEqualTo(0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl library/member -Dtest=MemberControllerTest --no-transfer-progress
```
Expected: FAIL — `MemberApplication` doesn't exist yet.

- [ ] **Step 3: Create `library/member/src/main/resources/application.properties`**

```properties
server.port=8082
spring.application.name=member
spring.datasource.url=jdbc:h2:mem:member
spring.h2.console.enabled=true
spring.kafka.bootstrap-servers=localhost:9092
management.health.kafka.enabled=false
management.health.binders.enabled=false
```

- [ ] **Step 4: Create `library/member/src/test/resources/application.properties`**

```properties
management.health.kafka.enabled=false
management.health.binders.enabled=false
```

- [ ] **Step 5: Create `MemberApplication.java`**

```java
package victor.training.library.member;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MemberApplication {
    public static void main(String[] args) {
        SpringApplication.run(MemberApplication.class, args);
    }
}
```

- [ ] **Step 6: Create `MemberBook.java`**

```java
package victor.training.library.member;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@Table(uniqueConstraints = @UniqueConstraint(columnNames = {"checkoutId", "bookId"}))
public class MemberBook {
    @Id @GeneratedValue
    private Long id;
    private long userId;
    private long bookId;
    private String checkoutId;

    public MemberBook(long userId, long bookId, String checkoutId) {
        this.userId = userId;
        this.bookId = bookId;
        this.checkoutId = checkoutId;
    }
}
```

- [ ] **Step 7: Create `MemberBookRepo.java`**

```java
package victor.training.library.member;

import org.springframework.data.jpa.repository.JpaRepository;

public interface MemberBookRepo extends JpaRepository<MemberBook, Long> {
    long countByUserId(long userId);
    boolean existsByCheckoutIdAndBookId(String checkoutId, long bookId);
    void deleteByUserIdAndCheckoutId(long userId, String checkoutId);
}
```

- [ ] **Step 8: Create `MemberController.java`**

```java
package victor.training.library.member;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class MemberController {
    private final MemberBookRepo repo;

    record AddBooksRequest(String checkoutId, List<Long> bookIds) {}

    @PostMapping("/members/{userId}/books")
    @Transactional
    public void addBooks(@PathVariable long userId, @RequestBody AddBooksRequest request) {
        long existing = repo.countByUserId(userId);
        if (existing + request.bookIds().size() > 5) {
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "Member %d already has %d books (max 5)".formatted(userId, existing));
        }
        for (long bookId : request.bookIds()) {
            if (!repo.existsByCheckoutIdAndBookId(request.checkoutId(), bookId)) {
                repo.save(new MemberBook(userId, bookId, request.checkoutId()));
            }
        }
    }

    @DeleteMapping("/members/{userId}/books/{checkoutId}")
    @Transactional
    public void removeBooks(@PathVariable long userId, @PathVariable String checkoutId) {
        repo.deleteByUserIdAndCheckoutId(userId, checkoutId);
    }
}
```

- [ ] **Step 9: Run tests — expect all pass**

```bash
mvn test -pl library/member --no-transfer-progress
```
Expected: 4 tests PASS.

- [ ] **Step 10: Create `Events.java` Kafka stub**

```java
package victor.training.library.member;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, consume BookCheckedOut instead of the REST endpoint

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "member")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     memberController.addBooks(event.userId(),
    //         new MemberController.AddBooksRequest(event.checkoutId(), event.bookIds()));
    // }
}
```

- [ ] **Step 11: Commit**

```bash
git add library/member/
git commit -m "feat: add Member service with idempotent book checkout and compensation endpoint"
```

---

### Task 3: Teleport service

**Files:**
- Create: `library/teleport/src/main/java/victor/training/library/teleport/TeleportApplication.java`
- Create: `library/teleport/src/main/java/victor/training/library/teleport/TeleportController.java`
- Create: `library/teleport/src/main/java/victor/training/library/teleport/Events.java`
- Create: `library/teleport/src/main/resources/application.properties`
- Create: `library/teleport/src/test/java/victor/training/library/teleport/TeleportControllerTest.java`
- Create: `library/teleport/src/test/resources/application.properties`

- [ ] **Step 1: Write failing tests**

Create `library/teleport/src/test/java/victor/training/library/teleport/TeleportControllerTest.java`:

```java
package victor.training.library.teleport;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TeleportControllerTest {
    @Autowired MockMvc mockMvc;

    @Test
    void teleports_successfully() throws Exception {
        mockMvc.perform(post("/teleport")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""{"userId":1,"bookIds":[1,2]}"""))
                .andExpect(status().isOk());
    }

    @Test
    void toggleFail_makesNextCallFail() throws Exception {
        mockMvc.perform(post("/teleport/toggle-fail")).andExpect(status().isOk());

        mockMvc.perform(post("/teleport")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""{"userId":1,"bookIds":[1,2]}"""))
                .andExpect(status().isInternalServerError());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl library/teleport -Dtest=TeleportControllerTest --no-transfer-progress
```
Expected: FAIL — `TeleportApplication` doesn't exist yet.

- [ ] **Step 3: Create `library/teleport/src/main/resources/application.properties`**

```properties
server.port=8083
spring.application.name=teleport
spring.kafka.bootstrap-servers=localhost:9092
management.health.kafka.enabled=false
management.health.binders.enabled=false
```

- [ ] **Step 4: Create `library/teleport/src/test/resources/application.properties`**

```properties
management.health.kafka.enabled=false
management.health.binders.enabled=false
```

- [ ] **Step 5: Create `TeleportApplication.java`**

```java
package victor.training.library.teleport;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TeleportApplication {
    public static void main(String[] args) {
        SpringApplication.run(TeleportApplication.class, args);
    }
}
```

- [ ] **Step 6: Create `TeleportController.java`**

```java
package victor.training.library.teleport;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Slf4j
@RestController
public class TeleportController {
    private boolean failNext = false;

    record TeleportRequest(long userId, List<Long> bookIds) {}

    @PostMapping("/teleport")
    public void teleport(@RequestBody TeleportRequest request) {
        if (failNext) {
            failNext = false;
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR,
                    "Teleporter malfunction! Books remain in warehouse.");
        }
        log.info("Teleporting books {} to user {}'s living room", request.bookIds(), request.userId());
    }

    @PostMapping("/teleport/toggle-fail")
    public String toggleFail() {
        failNext = !failNext;
        return "Teleporter will " + (failNext ? "FAIL" : "succeed") + " on next call";
    }
}
```

- [ ] **Step 7: Run tests — expect both pass**

```bash
mvn test -pl library/teleport --no-transfer-progress
```
Expected: 2 tests PASS.

- [ ] **Step 8: Create `Events.java` Kafka stub**

```java
package victor.training.library.teleport;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, consume BookCheckedOut instead of the REST endpoint

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "teleport")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     teleportController.teleport(new TeleportController.TeleportRequest(event.userId(), event.bookIds()));
    // }
}
```

- [ ] **Step 9: Commit**

```bash
git add library/teleport/
git commit -m "feat: add Teleport service with toggle-fail endpoint for compensation demo"
```

---

### Task 4: Checkout service

**Files:**
- Create: `library/checkout/src/main/java/victor/training/library/checkout/CheckoutApplication.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/CheckoutRequest.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/CheckoutController.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/CheckoutService.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/MemberClient.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/TeleportClient.java`
- Create: `library/checkout/src/main/java/victor/training/library/checkout/Events.java`
- Create: `library/checkout/src/main/resources/application.properties`
- Create: `library/checkout/src/test/java/victor/training/library/checkout/CheckoutServiceTest.java`

- [ ] **Step 1: Write failing unit tests for compensation logic**

Create `library/checkout/src/test/java/victor/training/library/checkout/CheckoutServiceTest.java`:

```java
package victor.training.library.checkout;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CheckoutServiceTest {
    @Mock MemberClient memberClient;
    @Mock TeleportClient teleportClient;
    @InjectMocks CheckoutService checkoutService;

    @Test
    void happyPath_doesNotCallCompensation() {
        checkoutService.checkout(new CheckoutRequest(1L, List.of(1L, 6L)));

        verify(memberClient).addBooks(eq(1L), any(String.class), eq(List.of(1L, 6L)));
        verify(teleportClient).teleport(eq(1L), eq(List.of(1L, 6L)));
        verify(memberClient, never()).removeBooks(anyLong(), anyString());
    }

    @Test
    void compensation_removesBooks_whenTeleportFails() {
        doThrow(new RuntimeException("teleporter malfunction"))
                .when(teleportClient).teleport(anyLong(), anyList());

        assertThatThrownBy(() -> checkoutService.checkout(new CheckoutRequest(1L, List.of(1L, 6L))))
                .isInstanceOf(RuntimeException.class);

        verify(memberClient).addBooks(eq(1L), any(String.class), eq(List.of(1L, 6L)));
        verify(memberClient).removeBooks(eq(1L), any(String.class));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
mvn test -pl library/checkout -Dtest=CheckoutServiceTest --no-transfer-progress
```
Expected: FAIL — `CheckoutService`, `MemberClient`, `TeleportClient`, `CheckoutRequest` don't exist yet.

- [ ] **Step 3: Create `library/checkout/src/main/resources/application.properties`**

```properties
server.port=8080
spring.application.name=checkout
spring.kafka.bootstrap-servers=localhost:9092
management.health.kafka.enabled=false
management.health.binders.enabled=false
member.service.url=http://localhost:8082
teleport.service.url=http://localhost:8083
```

- [ ] **Step 4: Create `CheckoutApplication.java`**

```java
package victor.training.library.checkout;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CheckoutApplication {
    public static void main(String[] args) {
        SpringApplication.run(CheckoutApplication.class, args);
    }
}
```

- [ ] **Step 5: Create `CheckoutRequest.java`**

```java
package victor.training.library.checkout;

import java.util.List;

public record CheckoutRequest(long userId, List<Long> bookIds) {}
```

- [ ] **Step 6: Create `MemberClient.java`**

```java
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
```

- [ ] **Step 7: Create `TeleportClient.java`**

```java
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
```

- [ ] **Step 8: Create `CheckoutService.java`**

```java
package victor.training.library.checkout;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@RequiredArgsConstructor
public class CheckoutService {
    private final MemberClient memberClient;
    private final TeleportClient teleportClient;

    public void checkout(CheckoutRequest request) {
        String checkoutId = UUID.randomUUID().toString();
        memberClient.addBooks(request.userId(), checkoutId, request.bookIds());
        try {
            teleportClient.teleport(request.userId(), request.bookIds());
        } catch (Exception e) {
            memberClient.removeBooks(request.userId(), checkoutId);
            throw e;
        }
    }
}
```

- [ ] **Step 9: Create `CheckoutController.java`**

```java
package victor.training.library.checkout;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class CheckoutController {
    private final CheckoutService checkoutService;

    @PostMapping("/checkout")
    public void checkout(@RequestBody CheckoutRequest request) {
        checkoutService.checkout(request);
    }
}
```

- [ ] **Step 10: Run tests — expect both pass**

```bash
mvn test -pl library/checkout --no-transfer-progress
```
Expected: 2 tests PASS.

- [ ] **Step 11: Create `Events.java` Kafka stub**

```java
package victor.training.library.checkout;

import java.util.List;

public class Events {
    // TODO: When converting to Kafka, publish BookCheckedOut instead of calling Member + Teleport directly

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @Autowired KafkaTemplate<String, BookCheckedOut> kafkaTemplate;
    //
    // Replace the try/catch in CheckoutService with:
    //   kafkaTemplate.send("book-checked-out", checkoutId,
    //       new BookCheckedOut(checkoutId, request.userId(), request.bookIds()));
}
```

- [ ] **Step 12: Commit**

```bash
git add library/checkout/
git commit -m "feat: add Checkout service with RestClient orchestration and compensation"
```

---

### Task 5: Reminder stub

**Files:**
- Create: `library/reminder/src/main/java/victor/training/library/reminder/ReminderApplication.java`
- Create: `library/reminder/src/main/java/victor/training/library/reminder/Events.java`
- Create: `library/reminder/src/main/resources/application.properties`

- [ ] **Step 1: Create `ReminderApplication.java`**

```java
package victor.training.library.reminder;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ReminderApplication {
    public static void main(String[] args) {
        SpringApplication.run(ReminderApplication.class, args);
    }
}
```

- [ ] **Step 2: Create `library/reminder/src/main/resources/application.properties`**

```properties
server.port=8084
spring.application.name=reminder
spring.kafka.bootstrap-servers=localhost:9092
management.health.kafka.enabled=false
management.health.binders.enabled=false
```

- [ ] **Step 3: Create `Events.java` Kafka stub**

```java
package victor.training.library.reminder;

import java.util.List;

public class Events {
    // TODO: Connect Reminder to Kafka to schedule return reminders

    record BookCheckedOut(String checkoutId, long userId, List<Long> bookIds) {}

    // @KafkaListener(topics = "book-checked-out", groupId = "reminder")
    // public void onBookCheckedOut(BookCheckedOut event) {
    //     // Schedule a reminder for event.userId() to return event.bookIds() in 14 days
    //     log.info("Scheduling return reminder for user {} for books {}", event.userId(), event.bookIds());
    // }
}
```

- [ ] **Step 4: Verify Reminder starts**

```bash
mvn spring-boot:run -pl library/reminder
```
Expected: starts on port 8084 with no errors. Press Ctrl+C to stop.

- [ ] **Step 5: Commit**

```bash
git add library/reminder/
git commit -m "feat: add Reminder stub with Kafka consumer scaffold"
```

---

### Task 6: requests.http entry point and full build

**Files:**
- Create: `library/requests.http`

- [ ] **Step 1: Create `library/requests.http`**

```http
### Happy path — checkout 2 books for user 1
POST http://localhost:8080/checkout
Content-Type: application/json

{"userId": 1, "bookIds": [1, 6]}

###

### Check member account via H2 console
### Open http://localhost:8082/h2-console
### JDBC URL: jdbc:h2:mem:member   User: sa   Password: (empty)

###

### Make teleport FAIL on next call
POST http://localhost:8083/teleport/toggle-fail

###

### Checkout again — teleport fails, compensation removes books from member account
POST http://localhost:8080/checkout
Content-Type: application/json

{"userId": 1, "bookIds": [2, 3]}

###

### Try to exceed 5-book limit (expects 409 Conflict)
POST http://localhost:8080/checkout
Content-Type: application/json

{"userId": 1, "bookIds": [10, 11, 12, 13, 14, 15]}
```

- [ ] **Step 2: Full Maven build**

```bash
mvn clean install --no-transfer-progress
```
Expected: `BUILD SUCCESS` — all modules compile and all tests pass.

- [ ] **Step 3: Smoke test with all 4 services running**

Open 4 terminals and start each service:
```bash
# Terminal 1
mvn spring-boot:run -pl library/member

# Terminal 2
mvn spring-boot:run -pl library/teleport

# Terminal 3
mvn spring-boot:run -pl library/checkout

# Terminal 4
mvn spring-boot:run -pl library/reminder
```

Open `library/requests.http` in IntelliJ and run the requests in order. Verify:
1. First checkout → 200 OK, H2 console shows 2 rows for userId=1
2. Toggle-fail → 200 OK
3. Second checkout → 500, H2 console shows no new rows (compensation worked)
4. Exceed limit → 409 Conflict

- [ ] **Step 4: Commit**

```bash
git add library/requests.http
git commit -m "feat: add requests.http workshop entry point for library scenario"
```
