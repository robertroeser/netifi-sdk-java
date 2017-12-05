package io.netifi.sdk.balancer;

import io.netifi.sdk.balancer.transport.WeighedClientTransportSupplier;
import io.netifi.sdk.rs.WeightedRSocket;
import java.net.InetSocketAddress;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.DirectProcessor;

public class WeighedClientTransportSupplierTest {
  @Test
  public void testShouldDecrementActiveCountOnComplete() {
    WeighedClientTransportSupplier supplier =
        new WeighedClientTransportSupplier(
            () -> null, InetSocketAddress.createUnresolved("localhost", 8081));

    DirectProcessor<WeightedRSocket> p = DirectProcessor.create();
    supplier.apply(p).get();

    int i = supplier.activeConnections();

    Assert.assertEquals(1, i);

    p.onComplete();

    i = supplier.activeConnections();

    Assert.assertEquals(0, i);
  }

  @Test
  public void testShouldDecrementActiveCountOnError() {
    WeighedClientTransportSupplier supplier =
        new WeighedClientTransportSupplier(
            () -> null, InetSocketAddress.createUnresolved("localhost", 8081));

    DirectProcessor<WeightedRSocket> p = DirectProcessor.create();
    supplier.apply(p).get();

    int i = supplier.activeConnections();

    Assert.assertEquals(1, i);

    p.onError(new RuntimeException());

    i = supplier.activeConnections();

    Assert.assertEquals(0, i);
  }
}
