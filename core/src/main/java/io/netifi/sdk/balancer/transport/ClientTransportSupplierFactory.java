package io.netifi.sdk.balancer.transport;

import io.netifi.sdk.connection.DiscoveryEvent;
import io.rsocket.Closeable;
import io.rsocket.transport.ClientTransport;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * Factory that produces {@link WeighedClientTransportSupplier} implementations. Uses a Flux of
 * {@link DiscoveryEvent} to determine which {@link WeighedClientTransportSupplier} to provide.
 * Selects the provides based on their weighted score.
 *
 * @see DiscoveryEvent
 * @see WeighedClientTransportSupplier
 */
public class ClientTransportSupplierFactory
    implements Closeable, Supplier<Mono<WeighedClientTransportSupplier>> {
  private static final Logger logger =
      LoggerFactory.getLogger(ClientTransportSupplierFactory.class);
  private final Disposable subscribe;
  private final MonoProcessor<Void> onClose;
  private final List<WeighedClientTransportSupplier> suppliers;
  private final Function<SocketAddress, Supplier<ClientTransport>> factory;
  private MonoProcessor<Void> onSuppliersPresent;
  private boolean missed = false;

  public ClientTransportSupplierFactory(
      Flux<DiscoveryEvent> events, Function<SocketAddress, Supplier<ClientTransport>> factory) {
    resetSuppliersPresent();
    this.onClose = MonoProcessor.create();
    this.factory = factory;
    this.suppliers = new ArrayList<>();
    this.subscribe =
        events
            .doOnNext(this::handelEvent)
            .doOnError(t -> logger.error(t.getMessage(), t))
            .retry()
            .subscribe();
  }

  private synchronized void resetSuppliersPresent() {
    onSuppliersPresent = MonoProcessor.create();
  }

  private synchronized Mono<Void> onSuppliersPresent() {
    return onSuppliersPresent;
  }

  private synchronized void handelEvent(DiscoveryEvent event) {
    missed = true;
    SocketAddress address = event.getAddress();
    if (event == DiscoveryEvent.Add) {
      logger.debug("adding client supplier for socket address {}", address);
      WeighedClientTransportSupplier supplier =
          new WeighedClientTransportSupplier(factory.apply(address), address);
      suppliers.add(supplier);
    } else {
      logger.debug("remove client supplier for socket address {}", address);
      suppliers.removeIf(s -> s.getSocketAddress().equals(address));
    }

    if (suppliers.isEmpty()) {
      logger.debug("no client transport suppliers present, reset to wait for suppliers");
      resetSuppliersPresent();
    } else {
      if (!onSuppliersPresent.isTerminated()) {
        logger.debug("notifying client transport suppliers are present");
        onSuppliersPresent.onComplete();
      }
    }
  }

  public Mono<WeighedClientTransportSupplier> get() {
    return onSuppliersPresent().then(Mono.fromSupplier(this::select));
  }

  private WeighedClientTransportSupplier select() {
    WeighedClientTransportSupplier supplier;

    for (; ; ) {
      List<WeighedClientTransportSupplier> _s;
      synchronized (this) {
        missed = false;
        _s = suppliers;
      }

      int size = _s.size();
      if (size == 1) {
        supplier = _s.get(0);
      } else {
        WeighedClientTransportSupplier supplier1 = null;
        WeighedClientTransportSupplier supplier2 = null;

        Random rnd = ThreadLocalRandom.current();
        int i1 = rnd.nextInt(size);
        int i2 = rnd.nextInt(size - 1);
        if (i2 >= i1) {
          i2++;
        }

        supplier1 = _s.get(i1);
        supplier2 = _s.get(i2);

        double w1 = supplier1.weight();
        double w2 = supplier2.weight();

        supplier = w1 < w2 ? supplier2 : supplier1;
      }

      synchronized (this) {
        if (!missed) {
          break;
        }
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("selecting socket {}", supplier.toString());
    }

    return supplier;
  }

  @Override
  public Mono<Void> close() {
    return Mono.fromRunnable(onClose::onComplete).doFinally(s -> subscribe.dispose()).then();
  }

  @Override
  public Mono<Void> onClose() {
    return onClose;
  }

  @Override
  public String toString() {
    return "ClientTransportSupplierFactory{"
        + "subscribe="
        + subscribe.isDisposed()
        + ", onClose="
        + onClose.isTerminated()
        + ", onSuppliersPresent="
        + onSuppliersPresent.isSuccess()
        + ", missed="
        + missed
        + '}';
  }
}
