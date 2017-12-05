package io.netifi.sdk.rs;

import io.netifi.sdk.Netifi;
import io.netifi.testing.protobuf.*;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Ignore
public class ProteusRoutingIntegrationTest {

  private static final long accessKey = 3855261330795754807L;
  private static final String accessToken = "n9R9042eE1KaLtE56rbWjBIGymo=";

  private static Netifi server;
  private static Netifi client;
  private static NetifiSocket netifiSocket;

  @BeforeClass
  public static void setup() {
    server =
        Netifi.builder()
            .group("test.server")
            .destination("server")
            .host("edge.prd.netifi.io")
            .accountId(Long.MAX_VALUE)
            .accessKey(accessKey)
            .accessToken(accessToken)
            .build();

    client =
        Netifi.builder()
            .group("test.client")
            .destination("client")
            .host("edge.prd.netifi.io")
            .accountId(Long.MAX_VALUE)
            .accessKey(accessKey)
            .accessToken(accessToken)
            .build();

    server.addService(
        new SimpleServiceServer(new ProteusLocalRoutingIntegrationTest.DefaultSimpleService()));

    netifiSocket = client.connect("test.server").block();
  }

  @Test
  public void testUnaryRpc() {
    SimpleServiceClient simpleServiceClient = new SimpleServiceClient(netifiSocket);
    SimpleResponse simpleResponse =
        simpleServiceClient
            .unaryRpc(SimpleRequest.newBuilder().setRequestMessage("a message").build())
            .block();

    System.out.println(simpleResponse.getResponseMessage());
  }

  @Test
  public void testServerStreamingRpc() {
    SimpleServiceClient simpleServiceClient = new SimpleServiceClient(netifiSocket);
    SimpleResponse response =
        simpleServiceClient
            .serverStreamingRpc(SimpleRequest.newBuilder().setRequestMessage("a message").build())
            .take(1000)
            .blockLast();

    System.out.println(response.getResponseMessage());
  }

  @Test
  public void testClientStreamingRpc() {
    SimpleServiceClient simpleServiceClient = new SimpleServiceClient(netifiSocket);
    SimpleResponse response =
        simpleServiceClient
            .clientStreamingRpc(
                Flux.range(1, 11)
                    .map(
                        i ->
                            SimpleRequest.newBuilder().setRequestMessage("a message " + i).build()))
            .block();

    System.out.println(response.getResponseMessage());
  }

  @Test
  public void testBidiRequest() {
    SimpleServiceClient simpleServiceClient = new SimpleServiceClient(netifiSocket);

    Flux<SimpleRequest> map =
        Flux.range(1, 100)
            .publishOn(Schedulers.parallel(), 32)
            .map(i -> SimpleRequest.newBuilder().setRequestMessage("a message -> " + i).build());

    SimpleResponse response =
        simpleServiceClient
            .bidiStreamingRpc(map)
            .doOnNext(simpleResponse -> System.out.println(simpleResponse.getResponseMessage()))
            .blockLast();

    System.out.println(response.getResponseMessage());
  }

  @Test
  public void testFireAndForget() throws Exception {
    int count = 1000;
    CountDownLatch latch = new CountDownLatch(count);
    SimpleServiceClient client = new SimpleServiceClient(netifiSocket);
    client
        .streamOnFireAndForget(Empty.getDefaultInstance())
        .subscribe(simpleResponse -> latch.countDown());
    Flux.range(1, count)
        .flatMap(
            i ->
                client.fireAndForget(
                    SimpleRequest.newBuilder().setRequestMessage("fire -> " + i).build()))
        .subscribe();
    latch.await();
  }

  static class DefaultSimpleService implements SimpleService {
    EmitterProcessor<SimpleRequest> messages = EmitterProcessor.create();

    @Override
    public Mono<Void> fireAndForget(SimpleRequest message) {
      messages.onNext(message);
      return Mono.empty();
    }

    @Override
    public Flux<SimpleResponse> streamOnFireAndForget(Empty message) {
      return messages.map(
          simpleRequest ->
              SimpleResponse.newBuilder()
                  .setResponseMessage("got fire and forget -> " + simpleRequest.getRequestMessage())
                  .build());
    }

    @Override
    public Mono<SimpleResponse> unaryRpc(SimpleRequest message) {
      return Mono.fromCallable(
          () ->
              SimpleResponse.newBuilder()
                  .setResponseMessage("we got the message -> " + message.getRequestMessage())
                  .build());
    }

    @Override
    public Mono<SimpleResponse> clientStreamingRpc(Publisher<SimpleRequest> messages) {
      return Flux.from(messages)
          .windowTimeout(10, Duration.ofSeconds(500))
          .take(1)
          .flatMap(Function.identity())
          .reduce(
              new ConcurrentHashMap<Character, AtomicInteger>(),
              (map, s) -> {
                char[] chars = s.getRequestMessage().toCharArray();
                for (char c : chars) {
                  map.computeIfAbsent(c, _c -> new AtomicInteger()).incrementAndGet();
                }

                return map;
              })
          .map(
              map -> {
                StringBuilder builder = new StringBuilder();

                map.forEach(
                    (character, atomicInteger) -> {
                      builder
                          .append("character -> ")
                          .append(character)
                          .append(", count -> ")
                          .append(atomicInteger.get())
                          .append("\n");
                    });

                String s = builder.toString();

                return SimpleResponse.newBuilder().setResponseMessage(s).build();
              });
    }

    @Override
    public Flux<SimpleResponse> serverStreamingRpc(SimpleRequest message) {
      String requestMessage = message.getRequestMessage();
      return Flux.interval(Duration.ofMillis(1))
          .onBackpressureDrop()
          .map(i -> i + " - got message - " + requestMessage)
          .map(s -> SimpleResponse.newBuilder().setResponseMessage(s).build());
    }

    @Override
    public Flux<SimpleResponse> bidiStreamingRpc(Publisher<SimpleRequest> messages) {
      return Flux.from(messages).flatMap(this::unaryRpc);
    }
  }
}
