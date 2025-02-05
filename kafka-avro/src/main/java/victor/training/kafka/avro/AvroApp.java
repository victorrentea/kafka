package victor.training.kafka.avro;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class AvroApp {
  public static void main(String[] args) {
    SpringApplication.run(AvroApp.class, args);
  }

  @Autowired
  private KafkaTemplate<String, Employee> kafkaTemplate;

  @EventListener(ApplicationStartedEvent.class)
  public void printAppStarted() {
    System.out.println("App started");
    Employee employee = new Employee();
    employee.setName("John");
    employee.setAge(30);
//    employee.setPhone("012124131");
    kafkaTemplate.send("employee", employee); // key-less send
    System.out.println("Message sent");
  }

  @KafkaListener(topics = "employee")
  public void consume(ConsumerRecord<String, Employee> employee) {
    System.out.println("Consumed: " + employee.value());
  }

  @Bean
  public NewTopic myTopic() {
    return TopicBuilder.name("employee").build();
  }
}
