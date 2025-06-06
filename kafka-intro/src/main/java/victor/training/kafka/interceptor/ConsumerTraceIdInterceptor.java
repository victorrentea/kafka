package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

@Slf4j
public class ConsumerTraceIdInterceptor implements RecordInterceptor<Object, Object> {
  @Override
  public ConsumerRecord<Object, Object> intercept(
      ConsumerRecord<Object, Object> record,
      Consumer<Object, Object> consumer) {

    var traceIdHeaders = record.headers().headers("traceId");
    if (traceIdHeaders.iterator().hasNext()) {
      var traceIdFromHeader = new String(traceIdHeaders.iterator().next().value());
      MDC.put("traceId", traceIdFromHeader);
    } else {
      log.warn("No traceId header found in received message");
    }
    log.info("Received (partition={}): {} -> record: {}", record.partition(), record.value(), record);
    return record;
  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    MDC.remove("traceId");
  }
}
