package victor.training.kafka.tracing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/tracing")
public class TracingDownstreamController {

  @GetMapping("/downstream")
  public String downstream() {
    log.info("STEP 5 — Downstream HTTP endpoint hit");
    return "downstream-ok";
  }
}
