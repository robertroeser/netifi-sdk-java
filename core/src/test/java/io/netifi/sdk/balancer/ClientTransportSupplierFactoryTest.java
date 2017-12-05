package io.netifi.sdk.balancer;

import io.netifi.sdk.balancer.transport.ClientTransportSupplierFactory;
import io.netifi.sdk.balancer.transport.WeighedClientTransportSupplier;
import io.netifi.sdk.connection.DiscoveryEvent;
import io.rsocket.transport.netty.client.TcpClientTransport;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

public class ClientTransportSupplierFactoryTest {
  @Test(expected = IllegalStateException.class)
  public void testShouldNotEmit() {
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(Flux.never(), address -> null);

    factory.get().block(Duration.ofMillis(100));
  }

  @Test(timeout = 3000)
  public void testShouldEmitAfterFluxEmits() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(events, address -> null);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();

    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));

    latch.await();
  }

  @Test(timeout = 3000, expected = IllegalStateException.class)
  public void testShouldEmitThenTimeout() throws Exception {
    UnicastProcessor<DiscoveryEvent> events = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(events, address -> null);

    factory.get().doOnSuccess(s -> latch.countDown()).subscribe();
    events.onNext(DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)));
    latch.await();
    events.onNext(DiscoveryEvent.remove(InetSocketAddress.createUnresolved("localhost", 8080)));
    factory.get().block(Duration.ofMillis(100));
  }

  @Test(timeout = 3000)
  public void testShouldEmitThreeUniqueFactories() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(events, address -> null);

    long count = Flux.range(1, 20).flatMap(i -> factory.get()).distinct().count().block();

    System.out.println(count);
  }

  @Test(timeout = 5000)
  public void testShouldSelectOneMoreThanAnother() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8080)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(events, address -> null);

    WeighedClientTransportSupplier block = factory.get().block();
    for (int i = 0; i < 2000; i++) {
      block.apply(Flux.never());
    }

    AtomicInteger i1 = new AtomicInteger();
    AtomicInteger i2 = new AtomicInteger();

    Flux.range(1, 20_000)
        .flatMap(i -> factory.get())
        .doOnNext(
            s -> {
              s.apply(Flux.never());
              if (s == block) {
                i1.incrementAndGet();
              } else {
                i2.incrementAndGet();
              }
            })
        .blockLast();

    Assert.assertTrue(i1.get() > 0);
    Assert.assertTrue(i1.get() < i2.get());
    Assert.assertTrue(i1.get() + i2.get() == 20_000);
  }

  @Test(timeout = 5000)
  @Ignore
  public void testShouldEventuallySelectOnlySocket() throws Exception {
    Flux<DiscoveryEvent> events =
        Flux.just(
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8001)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8081)),
            DiscoveryEvent.add(InetSocketAddress.createUnresolved("localhost", 8082)));

    ClientTransportSupplierFactory factory =
        new ClientTransportSupplierFactory(
            events,
            (SocketAddress address) ->
                () -> TcpClientTransport.create((InetSocketAddress) address));

    WeighedClientTransportSupplier block = factory.get().block();
    for (int i = 0; i < 2000; i++) {
      block.apply(Flux.never());
    }

    AtomicInteger i1 = new AtomicInteger();
    AtomicInteger i2 = new AtomicInteger();

    Flux.range(1, 20_000)
        .flatMap(i -> factory.get())
        .doOnNext(
            s -> {
              s.apply(Flux.never());
              if (s == block) {
                i1.incrementAndGet();
              } else {
                i2.incrementAndGet();
              }
            })
        .blockLast();

    Assert.assertTrue(i1.get() > 0);
    Assert.assertTrue(i1.get() < i2.get());
    Assert.assertTrue(i1.get() + i2.get() == 20_000);
  }
}
