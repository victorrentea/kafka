package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.Iterator;

@Slf4j
public class ConsumerRecordTrackingInterceptor implements RecordInterceptor<Object, Object> {
  @Override
  public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    // TODO get value of header 'traceId'
    // TODO delete
    Iterator<Header> headerValues = record.headers().headers("traceId").iterator();
    if (!headerValues.hasNext()) {
      log.warn("No traceId header found");
    } else {
      Header header = headerValues.next();
      String traceId = new String(header.value());
      log.trace("Stored traceId: {}", traceId);
      // TODO set on thread the traceId
      MDC.put("traceId", traceId); // TODO delete
    }
    log.info("Received: {} from partition {}", record.value(), record.partition());
    return record;
  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    // TODO clear traceId from thread
    MDC.remove("traceId"); // TODO delete
  }
}
