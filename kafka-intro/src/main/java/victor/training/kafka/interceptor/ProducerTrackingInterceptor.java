package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.MDC;

import java.util.Map;

@Slf4j
public class ProducerTrackingInterceptor implements ProducerInterceptor<String, Object> {

  @Override
  public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
    String headerValue = (String) MDC.get("traceId");
    if (headerValue != null) {
      log.info("Adding traceId header: " + headerValue);
      record.headers().add("traceId", headerValue.getBytes());
    } else {
      log.warn("No traceId header found");
    }
    log.info("Sending: " + record);
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    // No-op
  }

  @Override
  public void close() {
    // No-op
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // No-op
  }
}