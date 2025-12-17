package victor.training.kafka.testutil;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Example usage: add this as a field of your test class
 * {@code @RegisterExtension TimeExtension timeExtension = new TimeExtension("2019-09-29");}
 */
public class TimeExtension implements InvocationInterceptor {
  private LocalDateTime fixedTime;

  public TimeExtension() {
    this.fixedTime = LocalDateTime.now();
  }

  public void advanceTime(Duration duration) {
    this.fixedTime = fixedTime.plus(duration);
  }

  @Override
  public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
    try (var mocked = mockStatic(LocalDateTime.class, CALLS_REAL_METHODS)) { // other methods=untouched
      mocked.when(LocalDateTime::now).thenAnswer(call -> fixedTime); // -> allows change of date during @Test

      invocation.proceed();
    }
  }
}