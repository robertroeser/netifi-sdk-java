package io.netifi.sdk.presence;

import io.netifi.sdk.Netifi;
import io.netifi.sdk.frames.DestinationSetupFlyweight;
import io.netifi.sdk.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.PayloadImpl;
import java.time.Duration;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.ipc.netty.tcp.TcpClient;

@Ignore
public class DefaultPresenceNotifierTest {

  private static final long accessKey = 3855261330795754807L;
  private static final String accessToken = "n9R9042eE1KaLtE56rbWjBIGymo=";

  @Test
  public void testGroupPresence() throws Exception {

    RSocket rSocket = createRSocketConnection("tester", "test.groupPresence", 8001);

    DefaultPresenceNotifier handler =
        new DefaultPresenceNotifier(new TimebasedIdGenerator(1), accessKey, "tester", rSocket);

    handler.notify(Long.MAX_VALUE, "test.groupPresence").block();

    Netifi build =
        Netifi.builder()
            .accessKey(accessKey)
            .accountId(Long.MAX_VALUE)
            .accessToken(accessToken)
            .destination("test1")
            .group("anotherGroup")
            .host("127.0.0.1")
            .port(8001)
            .build();

    handler.notify(Long.MAX_VALUE, "anotherGroup").block();

    try {
      handler.notify(Long.MAX_VALUE, "anotherGroup2").timeout(Duration.ofSeconds(2)).block();
      Assert.fail();
    } catch (Throwable t) {
      if (!t.getMessage().contains("Timeout")) {
        Assert.fail();
      }
    }

    Netifi build2 =
        Netifi.builder()
            .accessKey(accessKey)
            .accountId(Long.MAX_VALUE)
            .accessToken(accessToken)
            .destination("test1")
            .group("anotherGroup2")
            .host("127.0.0.1")
            .port(8001)
            .build();

    handler.notify(Long.MAX_VALUE, "anotherGroup2").block();

    build.close().block();

    try {
      handler.notify(Long.MAX_VALUE, "anotherGroup").timeout(Duration.ofSeconds(2)).block();
      Assert.fail();
    } catch (Throwable t) {
      if (!t.getMessage().contains("Timeout")) {
        Assert.fail();
      }
    }
  }

  @Test
  public void testDestinationPresence() {
    RSocket rSocket = createRSocketConnection("tester", "test.destinationPresence", 8001);

    DefaultPresenceNotifier handler =
        new DefaultPresenceNotifier(new TimebasedIdGenerator(1), accessKey, "tester", rSocket);

    handler.notify(Long.MAX_VALUE, "tester", "test.destinationPresence").block();
  }

  public RSocket createRSocketConnection(String destination, String group, int port) {
    return createRSocketConnection(destination, group, new AbstractRSocket() {}, port);
  }

  public RSocket createRSocketConnection(
      String destination, String group, RSocket handler, int port) {
    int length = DestinationSetupFlyweight.computeLength(false, destination, group);
    byte[] bytes = new byte[length];

    byte[] accessTokenBytes = Base64.getDecoder().decode(accessToken);

    ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
    DestinationSetupFlyweight.encode(
        byteBuf,
        Unpooled.EMPTY_BUFFER,
        Unpooled.wrappedBuffer(accessTokenBytes),
        System.currentTimeMillis(),
        accessKey,
        destination,
        group);

    TcpClient tcpClient =
        TcpClient.create(
            builder -> {
              builder.option(ChannelOption.SO_RCVBUF, 1_048_576);
              builder.option(ChannelOption.SO_SNDBUF, 1_048_576);
              builder.option(ChannelOption.TCP_NODELAY, true);
              builder.option(ChannelOption.SO_KEEPALIVE, true);
              builder.host("127.0.0.1");
              builder.port(port);
              builder.build();
            });

    RSocket client =
        RSocketFactory.connect()
            .setupPayload(new PayloadImpl(new byte[0], bytes))
            .errorConsumer(Throwable::printStackTrace)
            .acceptor(rSocket -> handler)
            .transport(TcpClientTransport.create(tcpClient))
            .start()
            .block();

    return client;
  }
}