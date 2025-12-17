package victor.training.kafka.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

@Slf4j
public class ConsumerInterceptor implements RecordInterceptor<Object, Object> {
  @Override
  public ConsumerRecord<Object, Object> intercept(
      ConsumerRecord<Object, Object> record,
      Consumer<Object, Object> consumer) {

    var traceIdHeaders = record.headers().headers("traceId");
    if (traceIdHeaders.iterator().hasNext()) {
      var traceId = new String(traceIdHeaders.iterator().next().value());
      MDC.put("traceId", traceId);
    } else {
//      log.warn("No traceId header found in received message");
    }
    log.info("::START ({}-{}:{}), key:{}, value: {}", record.topic(),record.partition(), record.offset(), record.key(), record.value());
    return record;
  }

  @Override
  public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    log.info("::END::OK ({}-{}:{})", record.topic(),record.partition(), record.offset());
  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    MDC.remove("traceId");
  }

  @Override
  public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
    log.error("::END::ERROR ({}-{}:{}): {} caused by {}", record.topic(),record.partition(), record.offset(), exception.toString(), exception.getCause().toString());
  }
}
