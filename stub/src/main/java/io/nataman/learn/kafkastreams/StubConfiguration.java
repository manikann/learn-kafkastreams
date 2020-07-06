package io.nataman.learn.kafkastreams;

import static org.springframework.kafka.support.KafkaHeaders.MESSAGE_KEY;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

@Configuration(proxyBeanMethods = false)
@Log4j2
class StubConfiguration {
  static final AtomicBoolean runStub = new AtomicBoolean(true);

  @Bean
  BlockingQueue<PostingConfirmedEvent> postingResponseQueue() {
    return new LinkedTransferQueue<>();
  }

  @Bean
  Function<Message<BookingRequest>, Message<BookingResponse>> bookingServiceStub() {
    log.debug("bookingServiceStub created");
    return requestMessage -> {
      log.debug("bookingServiceStub: received {}", requestMessage);
      var request = requestMessage.getPayload();
      var response =
          BookingResponse.builder()
              .bookingRequestId(request.getBookingRequestId())
              .payload("Successfully booked")
              .status(Status.SUCCESS)
              .build();
      return MessageBuilder.withPayload(response)
          .setHeader(MESSAGE_KEY, request.getBookingRequestId())
          .build();
    };
  }

  @Bean
  Consumer<PostingConfirmedEvent> postingResponseConsumer(
      @Qualifier("postingResponseQueue")
          BlockingQueue<PostingConfirmedEvent> postingResponseQueue) {
    return postingConfirmedEvent -> {
      log.debug("postingResponseConsumer: {}", postingConfirmedEvent);
      postingResponseQueue.offer(postingConfirmedEvent);
    };
  }

  @Bean
  DefaultKafkaProducerFactoryCustomizer producerFactoryCustomizer() {
    return producerFactory -> {
      log.debug(
          "producerFactoryCustomizer: config={}", producerFactory.getConfigurationProperties());
      producerFactory.setValueSerializer(new JsonSerializer<>());
    };
  }
}
