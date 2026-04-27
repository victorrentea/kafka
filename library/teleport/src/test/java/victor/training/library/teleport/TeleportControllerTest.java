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
                        .content("""
                                {"userId":1,"bookIds":[1,2]}
                                """))
                .andExpect(status().isOk());
    }

    @Test
    void toggleFail_makesNextCallFail() throws Exception {
        mockMvc.perform(post("/teleport/toggle-fail")).andExpect(status().isOk());

        mockMvc.perform(post("/teleport")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"userId":1,"bookIds":[1,2]}
                                """))
                .andExpect(status().isInternalServerError());
    }
}
