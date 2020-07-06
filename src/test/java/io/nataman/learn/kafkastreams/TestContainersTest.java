package io.nataman.learn.kafkastreams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("testcontainers")
@Import({StubConfiguration.class})
@Log4j2
class TestContainersTest {
  static final String POSTING_REQUEST_TOPIC = "orchestrator.posting.request";
  static final String POSTING_RESPONSE_TOPIC = "posting.orchestrator.response";
  static final String BOOKING_REQUEST_TOPIC = "posting.booking.request";
  static final String BOOKING_RESPONSE_TOPIC = "booking.posting.response";
  static final String CORRELATION_LOG_TOPIC = "posting-booking-correlation-log";

  @Autowired
  @Qualifier("postingResponseQueue")
  BlockingQueue<PostingConfirmedEvent> postingResponseQueue;

  @Autowired KafkaTemplate kafkaTemplate;

  @SneakyThrows
  @Test
  void contextLoaded() {
    assertThat(kafkaTemplate).isNotNull();
    assertThat(postingResponseQueue).isNotNull();
    var request =
        PostingRequestedEvent.builder()
            .paymentUID("p1")
            .correlationId("c1")
            .payload("test-containers")
            .build();
    kafkaTemplate.send(POSTING_REQUEST_TOPIC, request.getPaymentUID(), request);
    await().atMost(Duration.ofSeconds(10)).until(() -> postingResponseQueue.size() == 1);
    log.debug("Final Response : {}", postingResponseQueue.peek());
  }
}
