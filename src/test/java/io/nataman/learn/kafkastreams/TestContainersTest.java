package io.nataman.learn.kafkastreams;

import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("testcontainers")
@Log4j2
@Import({TestConfiguration.class})
class TestContainersTest {

  @Test
  void contextLoaded() {}
}
