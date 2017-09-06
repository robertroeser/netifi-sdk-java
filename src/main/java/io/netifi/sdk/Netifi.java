package io.netifi.sdk;

import io.netifi.nrqp.frames.DestinationSetupFlyweight;
import io.netifi.sdk.annotations.Service;
import io.netifi.sdk.rs.RequestHandlingRSocket;
import io.netifi.sdk.util.HashUtil;
import io.netifi.sdk.util.TimebasedIdGenerator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivex.Flowable;
import io.reactivex.processors.ReplayProcessor;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.PayloadImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import javax.xml.bind.DatatypeConverter;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static io.netifi.sdk.util.HashUtil.hash;

/** This is where the magic happens */
public class Netifi implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Netifi.class);
  private final TimebasedIdGenerator idGenerator;
  private String host;
  private int port;
  private long accessKey;
  private long accountId;
  private long[] groupIds;
  private String destination;
  private long destinationId;
  private String group;
  private String accessToken;
  private byte[] accessTokenBytes;
  private volatile boolean running = true;
  private RequestHandlerRegistry registry;
  private ReplayProcessor<RSocket> rSocketPublishProcessor;
  private volatile Disposable disposable;

  private Netifi(
      String host,
      int port,
      long accessKey,
      long accountId,
      long[] groupIds,
      String destination,
      long destinationId,
      String group,
      String accessToken,
      byte[] accessTokenBytes) {
    this.registry = new DefaultRequestHandlerRegistry();
    this.host = host;
    this.port = port;
    this.accessKey = accessKey;
    this.accountId = accountId;
    this.groupIds = groupIds;
    this.destination = destination;
    this.destinationId = destinationId;
    this.group = group;
    this.accessToken = accessToken;
    this.accessTokenBytes = accessTokenBytes;
    this.rSocketPublishProcessor = ReplayProcessor.create(1);
    this.idGenerator = new TimebasedIdGenerator((int) destinationId);
  
    AtomicLong delay = new AtomicLong();
    
    this.disposable =
        Mono.defer(
                () -> {
                  int length = DestinationSetupFlyweight.computeLength(false, groupIds.length);
                  byte[] bytes = new byte[length];
                  ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
                  DestinationSetupFlyweight.encode(
                      byteBuf,
                      Unpooled.EMPTY_BUFFER,
                      Unpooled.wrappedBuffer(new byte[20]),
                      accountId,
                      destinationId,
                      idGenerator.nextId(),
                      groupIds);

                  return RSocketFactory.connect()
                      .errorConsumer(throwable -> logger.error("unhandled error", throwable))
                      .setupPayload(new PayloadImpl(new byte[0], bytes))
                      .acceptor(
                          rSocket -> {
                            rSocketPublishProcessor.onNext(rSocket);
                            return new RequestHandlingRSocket(registry);
                          })
                      .transport(TcpClientTransport.create(host, port))
                      .start()
                      .doOnSuccess(s -> delay.set(0));
                })
            .doOnError(Throwable::printStackTrace)
            .onErrorResume(t -> {
              long d = Math.min(10_000, delay.addAndGet(500));
              return Mono.delay(Duration.ofMillis(d)).then(Mono.error(t));
            })
            .retry(throwable -> running)
            .subscribe();
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "Service{"
        + "host='"
        + host
        + '\''
        + ", port="
        + port
        + ", accessKey="
        + accessKey
        + ", accountId="
        + accountId
        + ", groupIds="
        + Arrays.toString(groupIds)
        + ", destination='"
        + destination
        + '\''
        + ", destinationId="
        + destinationId
        + ", group='"
        + group
        + '\''
        + ", accessToken='"
        + accessToken
        + '\''
        + ", accessTokenBytes="
        + Arrays.toString(accessTokenBytes)
        + ", running="
        + running
        + '}';
  }

  public <T> T create(Class<T> service) {
    return create(service, -1);
  }

  public <T> T create(Class<T> service, long destination) {
    Objects.requireNonNull(service, "service must be non-null");
    Annotation[] annotations = service.getDeclaredAnnotations();

    if (annotations == null || annotations.length == 0) {
      throw new IllegalStateException("the class " + service.getName() + " has no annotations");
    }

    Service Service = null;
    for (Annotation annotation : annotations) {
      if (annotation instanceof Service) {
        Service = (Service) annotation;
        break;
      }
    }

    Objects.requireNonNull(Service, "no Service annotation found on " + service.getName());

    long accountId = Service.accountId();
    String group = Service.group();

    return create(service, accountId, group, destination);
  }

  public <T> T create(Class<T> service, long accountId, String group) {
    return create(service, accountId, group, -1);
  }

  /**
   * Routes to a group
   *
   * @param service
   * @param accountId
   * @param group
   * @param <T>
   * @return
   */
  public <T> T create(Class<T> service, long accountId, String group, long destination) {
    Objects.requireNonNull(service, "service must be non-null");
    Object o =
        Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            new Class<?>[] {service},
            new NetifiInvocationHandler(
                rSocketPublishProcessor,
                accountId,
                group,
                destination,
                this.accountId,
                this.groupIds,
                this.destinationId,
                idGenerator));

    return (T) o;
  }

  private void validate(Method method, Object[] args) {
    if (args.length != 1) {
      throw new IllegalStateException("methods can only have one argument");
    }

    Class<?> returnType = method.getReturnType();

    if (!returnType.isAssignableFrom(Flowable.class)) {
      throw new IllegalStateException("methods must return " + Flowable.class.getCanonicalName());
    }
  }

  public <T1, T2> void registerHandler(Class<T1> clazz, T2 t) {
    registry.registerHandler(clazz, t);
  }

  @Override
  public void close() throws Exception {
    if (disposable != null) {
      disposable.dispose();
    }
    running = false;
  }

  public static class Builder {
    private String host = "netifiedge.trafficmanager.net";
    private Integer port = 8001;
    private Long accessKey;
    private Long accountId;
    private String group;
    private long[] groupIds;
    private String destination;
    private Long destinationId;
    private String accessToken = null;
    private byte[] accessTokenBytes = new byte[20];

    private Builder() {}

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder accessKey(long accessKey) {
      this.accessKey = accessKey;
      return this;
    }

    public Builder accessToken(String accessToken) {
      this.accessToken = accessToken;
      this.accessTokenBytes = DatatypeConverter.parseBase64Binary(accessToken);
      return this;
    }

    public Builder group(String group) {
      this.group = group;
      String[] split = group.split("\\.");
      this.groupIds = new long[split.length];

      for (int i = 0; i < split.length; i++) {
        groupIds[i] = Math.abs(hash(split[i]));
      }

      return this;
    }

    public Builder accountId(long accountId) {
      this.accountId = accountId;
      return this;
    }

    public Builder destination(String destination) {
      this.destination = destination;
      this.destinationId = HashUtil.hash(destination);

      return this;
    }

    public Builder destinationId(long destinationId) {
      this.destinationId = destinationId;
      return this;
    }

    public Netifi build() {
      Objects.requireNonNull(host, "host is required");
      Objects.requireNonNull(port, "port is required");
      Objects.requireNonNull(accountId, "account Id is required");
      Objects.requireNonNull(group, "group is required");
      Objects.requireNonNull(destinationId, "destination id is required");

      return new Netifi(
          host,
          port,
          accessKey == null ? 0 : accessKey,
          accountId,
          groupIds,
          destination,
          destinationId,
          group,
          accessToken,
          accessTokenBytes);
    }
  }
}
