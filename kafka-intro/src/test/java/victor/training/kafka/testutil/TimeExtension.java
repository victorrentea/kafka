package victor.training.kafka.testutil;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;

/**
 * Example usage: add this as a field of your test class
 * {@code @RegisterExtension TimeExtension timeExtension = new TimeExtension("2019-09-29");}
 */
public class TimeExtension implements AfterEachCallback {
  private LocalDateTime fixedTime = LocalDateTime.now();
  private org.mockito.MockedStatic<LocalDateTime> mocked;

  public void advanceTime(Duration duration) {
    fixedTime = fixedTime.plus(duration);
    if (mocked == null) {
      // Lazily set up static mock only when time is actually manipulated,
      // to avoid interfering with instance mock tracking in tests that don't need it.
      mocked = mockStatic(LocalDateTime.class, CALLS_REAL_METHODS);
    }
    mocked.when(LocalDateTime::now).thenAnswer(call -> fixedTime);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    if (mocked != null) {
      mocked.close();
      mocked = null;
    }
    fixedTime = LocalDateTime.now();
  }
}
