package io.netifi.sdk.rs;

import io.netifi.sdk.auth.SessionUtil;
import io.netifi.sdk.frames.RouteDestinationFlyweight;
import io.netifi.sdk.frames.RouteType;
import io.netifi.sdk.frames.RoutingFlyweight;
import io.netifi.sdk.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.rsocket.Payload;
import io.rsocket.util.PayloadImpl;
import java.nio.ByteBuffer;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

public class DefaultNetifiSocket implements NetifiSocket {
  private final SessionUtil sessionUtil = SessionUtil.instance();
  private final MonoProcessor<Void> onClose;
  private final ByteBuf route;
  private long accessKey;
  private String fromDestination;
  private TimebasedIdGenerator generator;
  private ReconnectingRSocket reconnectingRSocket;

  public DefaultNetifiSocket(
      ReconnectingRSocket reconnectingRSocket,
      long accessKey,
      long fromAccountId,
      String fromDestination,
      String destination,
      String group,
      byte[] accessTokenBytes,
      boolean keepalive,
      TimebasedIdGenerator generator) {
    this.reconnectingRSocket = reconnectingRSocket;
    this.accessKey = accessKey;
    this.fromDestination = fromDestination;
    this.generator = generator;
    this.onClose = MonoProcessor.create();

    if (destination != null && !destination.equals("")) {
      int length =
          RouteDestinationFlyweight.computeLength(
              RouteType.STREAM_ID_ROUTE, fromDestination, group);
      route = Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(length));
      RouteDestinationFlyweight.encodeRouteByDestination(
          route, RouteType.STREAM_ID_ROUTE, fromAccountId, destination, group);
    } else {
      int length = RouteDestinationFlyweight.computeLength(RouteType.STREAM_GROUP_ROUTE, group);
      route = Unpooled.wrappedBuffer(ByteBuffer.allocateDirect(length));
      RouteDestinationFlyweight.encodeRouteByGroup(
          route, RouteType.STREAM_GROUP_ROUTE, fromAccountId, group);
    }

    reconnectingRSocket.onClose().doFinally(s -> onClose.onComplete()).subscribe();
  }

  @Override
  public double availability() {
    return reconnectingRSocket.availability();
  }

  public ByteBuf getRoute() {
    return route;
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      ByteBuf metadataToWrap = Unpooled.wrappedBuffer(payload.getMetadata());
      ByteBuf route = getRoute();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      return reconnectingRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return reconnectingRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, payload.getData(), count);
                          int requestToken =
                              sessionUtil.generateRequestToken(
                                  currentRequestToken, payload.getData(), count);
                          ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                          ByteBuf metadata = Unpooled.wrappedBuffer(byteBuffer);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return reconnectingRSocket.fireAndForget(
                              new PayloadImpl(payload.getData(), byteBuffer));
                        });
              });

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      ByteBuf metadataToWrap = Unpooled.wrappedBuffer(payload.getMetadata());
      ByteBuf route = getRoute();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      return reconnectingRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return reconnectingRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, payload.getData(), count);
                          int requestToken =
                              sessionUtil.generateRequestToken(
                                  currentRequestToken, payload.getData(), count);
                          ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                          ByteBuf metadata = Unpooled.wrappedBuffer(byteBuffer);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return reconnectingRSocket.requestResponse(
                              new PayloadImpl(payload.getData(), byteBuffer));
                        });
              });

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      ByteBuf metadataToWrap = Unpooled.wrappedBuffer(payload.getMetadata());
      ByteBuf route = getRoute();

      int length = RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

      return reconnectingRSocket
          .getCurrentSessionCounter()
          .flatMapMany(
              counter -> {
                long count = counter.incrementAndGet();

                return reconnectingRSocket
                    .getCurrentSessionToken()
                    .flatMapMany(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, payload.getData(), count);
                          int requestToken =
                              sessionUtil.generateRequestToken(
                                  currentRequestToken, payload.getData(), count);
                          ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                          ByteBuf metadata = Unpooled.wrappedBuffer(byteBuffer);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              metadataToWrap);

                          return reconnectingRSocket.requestStream(
                              new PayloadImpl(payload.getData(), byteBuffer));
                        });
              });

    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    ByteBuf route = getRoute();
    Flux<Payload> payloadFlux =
        Flux.from(payloads)
            .flatMap(
                payload -> {
                  ByteBuf metadataToWrap = Unpooled.wrappedBuffer(payload.getMetadata());
                  int length =
                      RoutingFlyweight.computeLength(true, fromDestination, route, metadataToWrap);

                  return reconnectingRSocket
                      .getCurrentSessionCounter()
                      .flatMapMany(
                          counter -> {
                            long count = counter.incrementAndGet();

                            return reconnectingRSocket
                                .getCurrentSessionToken()
                                .map(
                                    key -> {
                                      byte[] currentRequestToken =
                                          sessionUtil.generateSessionToken(
                                              key, payload.getData(), count);
                                      int requestToken =
                                          sessionUtil.generateRequestToken(
                                              currentRequestToken, payload.getData(), count);
                                      ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                                      ByteBuf metadata = Unpooled.wrappedBuffer(byteBuffer);
                                      RoutingFlyweight.encode(
                                          metadata,
                                          true,
                                          requestToken,
                                          accessKey,
                                          fromDestination,
                                          generator.nextId(),
                                          route,
                                          metadataToWrap);

                                      return new PayloadImpl(payload.getData(), byteBuffer);
                                    });
                          });
                });

    return reconnectingRSocket.requestChannel(payloadFlux);
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      ByteBuf route = getRoute();
      ByteBuf unwrappedMetadata = Unpooled.wrappedBuffer(payload.getMetadata());

      int length = RoutingFlyweight.computeLength(true, fromDestination, route);

      return reconnectingRSocket
          .getCurrentSessionCounter()
          .flatMap(
              counter -> {
                long count = counter.incrementAndGet();

                return reconnectingRSocket
                    .getCurrentSessionToken()
                    .flatMap(
                        key -> {
                          byte[] currentRequestToken =
                              sessionUtil.generateSessionToken(key, payload.getData(), count);
                          int requestToken =
                              sessionUtil.generateRequestToken(
                                  currentRequestToken, payload.getData(), count);
                          ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                          ByteBuf metadata = Unpooled.wrappedBuffer(byteBuffer);
                          RoutingFlyweight.encode(
                              metadata,
                              true,
                              requestToken,
                              accessKey,
                              fromDestination,
                              generator.nextId(),
                              route,
                              unwrappedMetadata);

                          return reconnectingRSocket.metadataPush(
                              new PayloadImpl(payload.getData(), byteBuffer));
                        });
              });

    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Void> close() {
    return Mono.fromRunnable(onClose::onComplete);
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }
}
