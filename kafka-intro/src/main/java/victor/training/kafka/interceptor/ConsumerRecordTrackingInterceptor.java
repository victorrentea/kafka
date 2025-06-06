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
  public ConsumerRecord<Object, Object> intercept(
      ConsumerRecord<Object, Object> record,
      Consumer<Object, Object> consumer) {
    Iterator<Header> headerValues = record.headers().headers("traceId").iterator();
    if (!headerValues.hasNext()) {
      log.warn("No traceId header found");
    } else {
      Header header = headerValues.next();
      String traceId = new String(header.value());
      log.trace("Stored traceId: {}", traceId);
      MDC.put("traceId", traceId);
    }
    log.info("Received (p={}): {} -> record: {}", record.partition(), record.value(), record);
    return record;
  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    MDC.remove("traceId");
  }
}
