package io.netifi.sdk.connection;

import java.net.SocketAddress;

/** DiscoveryEvent encapsulating when a address is added or removed */
public enum DiscoveryEvent {
  Add,
  Remove;

  private SocketAddress socketAddress;

  public static DiscoveryEvent add(SocketAddress socketAddress) {
    DiscoveryEvent add = DiscoveryEvent.Add;
    add.setAddress(socketAddress);
    return add;
  }

  public static DiscoveryEvent remove(SocketAddress socketAddress) {
    DiscoveryEvent remove = DiscoveryEvent.Remove;
    remove.setAddress(socketAddress);
    return remove;
  }

  public SocketAddress getAddress() {
    return socketAddress;
  }

  void setAddress(SocketAddress socketAddress) {
    this.socketAddress = socketAddress;
  }
}
