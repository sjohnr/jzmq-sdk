package org.zeromq.messaging;

import org.zeromq.support.ObjectBuilder;

import java.util.ArrayList;
import java.util.List;

public final class Props {

  private static final int DEFAULT_LINGER = 0; // by default it's allowed to close socket and not be blocked.
  private static final int DEFAULT_WAIT_ON_SEND = 1000; // how long to wait on .send(), best guess.
  private static final int DEFAULT_WAIT_ON_RECV = 1000; // how long to wait on .recv(), best guess.
  private static final long DEFAULT_HWM_SEND = 1000; // HWM, best guess.
  private static final long DEFAULT_HWM_RECV = 1000; // HWM, best guess.
  private static final long DEFAULT_RECONNECT_INTERVAL = 100; // reconnection interval, best guess.
  private static final long DEFAULT_RECONNECT_INTERVAL_MAX = 0; // reconnection interval max, best guess.

  public static final class Builder implements ObjectBuilder<Props> {

    private final Props _target = new Props();

    private Builder() {
    }

    public Builder withBindAddress(String address) {
      _target.bindAddresses.add(address);
      return this;
    }

    public Builder withBindAddresses(Iterable<String> addresses) {
      for (String address : addresses) {
        withBindAddress(address);
      }
      return this;
    }

    public Builder withConnAddress(String address) {
      _target.connectAddresses.add(address);
      return this;
    }

    public Builder withConnAddresses(Iterable<String> addresses) {
      for (String address : addresses) {
        withConnAddress(address);
      }
      return this;
    }

    public Builder withHwmSend(long hwm) {
      _target.hwmSend = hwm;
      return this;
    }

    public Builder withHwmRecv(long hwm) {
      _target.hwmRecv = hwm;
      return this;
    }

    public Builder withSocketIdPrefix(String socketIdPrefix) {
      _target.socketIdPrefix = socketIdPrefix;
      return this;
    }

    public Builder withLinger(long linger) {
      _target.linger = linger;
      return this;
    }

    public Builder withWaitSend(int timeout) {
      _target.timeoutSend = timeout;
      return this;
    }

    public Builder withWaitRecv(int timeout) {
      _target.timeoutRecv = timeout;
      return this;
    }

    public Builder withReconnectInterval(long reconnectInterval) {
      _target.reconnectInterval = reconnectInterval;
      return this;
    }

    public Builder withReconnectIntervalMax(long reconnectIntervalMax) {
      _target.reconnectIntervalMax = reconnectIntervalMax;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.bindAddresses.isEmpty() && _target.connectAddresses.isEmpty()) {
        throw ZmqException.fatal();
      }
    }

    @Override
    public Props build() {
      checkInvariant();
      return _target;
    }
  }

  private List<String> bindAddresses = new ArrayList<String>();
  private List<String> connectAddresses = new ArrayList<String>();
  private long hwmSend = DEFAULT_HWM_SEND;
  private long hwmRecv = DEFAULT_HWM_RECV;
  private String socketIdPrefix;
  private long linger = DEFAULT_LINGER;
  private int timeoutSend = DEFAULT_WAIT_ON_SEND;
  private int timeoutRecv = DEFAULT_WAIT_ON_RECV;
  private long reconnectInterval = DEFAULT_RECONNECT_INTERVAL;
  private long reconnectIntervalMax = DEFAULT_RECONNECT_INTERVAL_MAX;

  //// CONSTRUCTORS

  private Props() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setBindAddresses(List<String> bindAddresses) {
    this.bindAddresses = bindAddresses;
  }

  public void setConnectAddresses(List<String> connectAddresses) {
    this.connectAddresses = connectAddresses;
  }

  public void setHwmSend(long hwmSend) {
    this.hwmSend = hwmSend;
  }

  public void setHwmRecv(long hwmRecv) {
    this.hwmRecv = hwmRecv;
  }

  public void setSocketIdPrefix(String socketIdPrefix) {
    this.socketIdPrefix = socketIdPrefix;
  }

  public void setLinger(long linger) {
    this.linger = linger;
  }

  public void setTimeoutSend(int timeoutSend) {
    this.timeoutSend = timeoutSend;
  }

  public void setTimeoutRecv(int timeoutRecv) {
    this.timeoutRecv = timeoutRecv;
  }

  public void setReconnectInterval(long reconnectInterval) {
    this.reconnectInterval = reconnectInterval;
  }

  public void setReconnectIntervalMax(long reconnectIntervalMax) {
    this.reconnectIntervalMax = reconnectIntervalMax;
  }

  public List<String> getBindAddresses() {
    return bindAddresses;
  }

  public List<String> getConnectAddresses() {
    return connectAddresses;
  }

  public long getHwmSend() {
    return hwmSend;
  }

  public long getHwmRecv() {
    return hwmRecv;
  }

  public String getSocketIdPrefix() {
    return socketIdPrefix;
  }

  public long getLinger() {
    return linger;
  }

  public int getTimeoutSend() {
    return timeoutSend;
  }

  public int getTimeoutRecv() {
    return timeoutRecv;
  }

  public long getReconnectInterval() {
    return reconnectInterval;
  }

  public long getReconnectIntervalMax() {
    return reconnectIntervalMax;
  }
}
