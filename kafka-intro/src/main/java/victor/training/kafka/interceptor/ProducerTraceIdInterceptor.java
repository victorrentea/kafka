package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jboss.logging.MDC;

import java.util.Map;

@Slf4j
@SuppressWarnings("unused") // used in .yaml
public class ProducerTraceIdInterceptor implements ProducerInterceptor<String, Object> {

  @Override
  public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> record) {
    String traceIdFromCurrentThread = (String) MDC.get("traceId");
    if (traceIdFromCurrentThread != null) {
      log.info("Adding header: traceId={}", traceIdFromCurrentThread);
      record.headers().add("traceId", traceIdFromCurrentThread.getBytes());
    } else {
      log.debug("No traceId found on current thread");
    }
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