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
        String body = """
                {"checkoutId":"dup-id","bookIds":[10]}
                """;
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
                        .content("""
                                {"checkoutId":"over5","bookIds":[1,2,3,4,5,6]}
                                """))
                .andExpect(status().isConflict());
    }

    @Test
    void idempotent_retryAfterOtherCheckout_doesNotConflict() throws Exception {
        // first checkout: 2 books
        mockMvc.perform(post("/members/5/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"checkoutId":"first","bookIds":[1,2]}
                                """))
                .andExpect(status().isOk());

        // second checkout: 2 more books
        mockMvc.perform(post("/members/5/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"checkoutId":"second","bookIds":[3,4]}
                                """))
                .andExpect(status().isOk());

        // retry of first checkout must NOT return 409
        mockMvc.perform(post("/members/5/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"checkoutId":"first","bookIds":[1,2]}
                                """))
                .andExpect(status().isOk());

        assertThat(repo.countByUserId(5L)).isEqualTo(4);
    }

    @Test
    void compensation_removesAllBooksForCheckoutId() throws Exception {
        mockMvc.perform(post("/members/4/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"checkoutId":"to-undo","bookIds":[7,8]}
                                """))
                .andExpect(status().isOk());

        mockMvc.perform(delete("/members/4/books/to-undo"))
                .andExpect(status().isOk());

        assertThat(repo.countByUserId(4L)).isEqualTo(0);
    }
}
