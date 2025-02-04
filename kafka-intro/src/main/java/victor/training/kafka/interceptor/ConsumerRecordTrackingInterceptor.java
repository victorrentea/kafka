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
    Iterator<Header> it = record.headers().headers("traceId").iterator();
    if (!it.hasNext()) {
      log.warn("No traceId header found");
    } else {
      Header header = it.next();
      String value = new String(header.value());
      log.trace("Stored traceId: {}", value);
      MDC.put("traceId", value);
    }
    log.info("Received: {} from partition {}", record.value(), record.partition());
    return record;
  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    MDC.remove("traceId");
  }
}
