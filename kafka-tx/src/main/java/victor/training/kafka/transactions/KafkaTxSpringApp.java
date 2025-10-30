package victor.training.kafka.transactions;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;

@Slf4j
@SpringBootApplication
public class KafkaTxSpringApp {
  public static void main(String[] args) {
    SpringApplication.run(KafkaTxSpringApp.class, args);
  }

  @EventListener(ApplicationStartedEvent.class)
  public void onStartup() {
    log.info("⭐️⭐️⭐ APP STARTED ⭐️⭐️⭐️");
  }
}
