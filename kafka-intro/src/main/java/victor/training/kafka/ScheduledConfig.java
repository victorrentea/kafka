package victor.training.kafka;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;

@Slf4j
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT1H")
  // if a run didn't finish in 1 hour, allow re-starting it
@ConditionalOnProperty(name = "scheduled.enabled", havingValue = "true", matchIfMissing = true)
public class ScheduledConfig {
  @PostConstruct
  public void sayHello() {
    log.info("@Scheduled is enabled");
  }
  @Bean
  public LockProvider lockProvider(DataSource dataSource) {
    // uses a table created in schema.sql to synchronize instances
    return new JdbcTemplateLockProvider(dataSource);
  }
}
