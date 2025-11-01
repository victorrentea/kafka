package victor.training.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@EnableKafkaStreams
@RestController
@RequiredArgsConstructor
@SpringBootApplication
@EnableScheduling
public class StreamsApp {
  public static void main(String[] args) {
      SpringApplication.run(StreamsApp.class, args);
  }

}
