package io.netifi.sdk.balancer.transport;

import io.netifi.sdk.balancer.stats.Ewma;
import io.netifi.sdk.rs.WeightedRSocket;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.Clock;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class WeighedClientTransportSupplier
    implements Function<Flux<WeightedRSocket>, Supplier<ClientTransport>> {

  private static final Logger logger =
      LoggerFactory.getLogger(WeighedClientTransportSupplier.class);

  private static AtomicInteger SUPPLIER_ID = new AtomicInteger();

  private final int supplierId;

  private final Supplier<ClientTransport> internal;

  private final Ewma errorPercentage;

  private final Ewma latency;

  private final SocketAddress socketAddress;

  private final AtomicInteger activeConnections;

  public WeighedClientTransportSupplier(
      Supplier<ClientTransport> internal, SocketAddress socketAddress) {
    this.supplierId = SUPPLIER_ID.incrementAndGet();
    this.internal = internal;
    this.socketAddress = socketAddress;
    this.errorPercentage = new Ewma(5, TimeUnit.SECONDS, 1.0);
    this.latency = new Ewma(1, TimeUnit.MINUTES, Clock.unit().convert(1L, TimeUnit.SECONDS));
    this.activeConnections = new AtomicInteger();
  }

  @Override
  public Supplier<ClientTransport> apply(Flux<WeightedRSocket> weightedSocketFlux) {
    Disposable subscribe =
        weightedSocketFlux
            .doOnNext(
                weightedRSocket -> {
                  double e = weightedRSocket.errorPercentage();
                  double i = weightedRSocket.higherQuantileLatency();

                  errorPercentage.insert(e);
                  latency.insert(i);
                })
            .subscribe();

    return () ->
        () ->
            internal
                .get()
                .connect()
                .doOnNext(
                    duplexConnection -> {
                      int i = activeConnections.incrementAndGet();
                      logger.debug(
                          "supplier id - {} - opened connection to {} - active connections {}",
                          supplierId,
                          socketAddress,
                          i);

                      duplexConnection
                          .onClose()
                          .doFinally(
                              s -> {
                                int d = activeConnections.decrementAndGet();
                                logger.debug(
                                    "supplier id - {} - closed connection {} - active connections {}",
                                    supplierId,
                                    socketAddress,
                                    d);
                                subscribe.dispose();
                              })
                          .subscribe();
                    })
                .doOnError(t -> errorPercentage.insert(0.0));
  }

  public double errorPercentage() {
    return errorPercentage.value();
  }

  public double latency() {
    return latency.value();
  }

  public int activeConnections() {
    return activeConnections.get();
  }

  public double weight() {
    double e = errorPercentage();
    double l = latency();
    int a = activeConnections();

    return e * 1.0 / (1.0 + l * (a + 1));
  }

  public SocketAddress getSocketAddress() {
    return socketAddress;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    WeighedClientTransportSupplier supplier = (WeighedClientTransportSupplier) o;

    return socketAddress.equals(supplier.socketAddress);
  }

  @Override
  public int hashCode() {
    return socketAddress.hashCode();
  }

  @Override
  public String toString() {
    return "WeighedClientTransportSupplier{"
        + ", errorPercentage="
        + errorPercentage.value()
        + ", latency="
        + latency.value()
        + ", socketAddress="
        + socketAddress
        + ", activeConnections="
        + activeConnections.get()
        + '}';
  }
}
