package io.nataman.learn.kafkastreams;

import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@Log4j2
public class TestConfiguration {
  @Bean
  public Function<BookingRequest, BookingResponse> bookingServiceStub() {
    log.debug("bookingServiceStub created");
    return request -> {
      log.debug("bookingServiceStub: received {}", request);
      return BookingResponse.builder()
          .bookingRequestId(request.getBookingRequestId())
          .payload("Successfully booked")
          .status(Status.SUCCESS)
          .build();
    };
  }
}
