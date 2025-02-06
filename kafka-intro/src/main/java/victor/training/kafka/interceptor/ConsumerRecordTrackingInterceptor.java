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
      // TODO set on thread the traceId
    }
    log.info("Received: {} from partition {}", record.value(), record.partition());
    return record;
  }

//  @Override
//  public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
//    // TODO cum decorez exceptia sa adaug offset/partitie si text custom sa ridic in grafana alarma automat din log
////    throw new RuntimeException("🚨Message at "+ record.offset() + ":"+ record.partition() + " failed", exception);
//  }

  @Override
  public void afterRecord(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
    // TODO clear traceId from thread
  }
}
