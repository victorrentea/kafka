package victor.training.kafka.interceptor;

import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.amqp.RabbitTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//@Configuration
//class RabbitConfig { // rabbit equivalent (draft)
//  @Bean
//  RabbitTemplateCustomizer addBeforePublish() {
//    return template -> template.addBeforePublishPostProcessors(
//        message -> {
//          message.getMessageProperties().setHeader("traceId", MDC.get("traceId"));
//          return message;
//        });
//  }
//}