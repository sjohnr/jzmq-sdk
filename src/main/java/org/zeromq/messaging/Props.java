/*
 * Copyright (c) 2012 artem.vysochyn@gmail.com
 * Copyright (c) 2013 Other contributors as noted in the AUTHORS file
 *
 * jzmq-sdk is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 3 of the License, or
 * (at your option) any later version.
 *
 * jzmq-sdk is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * jzmq-sdk became possible because of jzmq binding and zmq library itself.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.zeromq.messaging;

import org.zeromq.support.ObjectBuilder;

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

    public Builder withBindAddr(String address) {
      _target.setBindAddr(address);
      return this;
    }

    public Builder withConnectAddr(String address) {
      _target.setConnectAddr(address);
      return this;
    }

    public Builder withHwmSend(long hwm) {
      _target.setHwmSend(hwm);
      return this;
    }

    public Builder withHwmRecv(long hwm) {
      _target.setHwmRecv(hwm);
      return this;
    }

    public Builder withSocketIdPrefix(String socketIdPrefix) {
      _target.setSocketIdPrefix(socketIdPrefix);
      return this;
    }

    public Builder withLinger(long linger) {
      _target.setLinger(linger);
      return this;
    }

    public Builder withWaitSend(int timeout) {
      _target.setTimeoutSend(timeout);
      return this;
    }

    public Builder withWaitRecv(int timeout) {
      _target.setTimeoutRecv(timeout);
      return this;
    }

    public Builder withReconnectInterval(long reconnectInterval) {
      _target.setReconnectInterval(reconnectInterval);
      return this;
    }

    public Builder withReconnectIntervalMax(long reconnectIntervalMax) {
      _target.setReconnectIntervalMax(reconnectIntervalMax);
      return this;
    }

    @Override
    public Props build() {
      return _target;
    }
  }

  private String bindAddr;
  private String connectAddr;
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

  public void setBindAddr(String bindAddr) {
    this.bindAddr = bindAddr;
  }

  public void setConnectAddr(String connectAddr) {
    this.connectAddr = connectAddr;
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

  public String bindAddr() {
    return bindAddr;
  }

  public String connectAddr() {
    return connectAddr;
  }

  public long hwmSend() {
    return hwmSend;
  }

  public long hwmRecv() {
    return hwmRecv;
  }

  public String socketIdPrefix() {
    return socketIdPrefix;
  }

  public long linger() {
    return linger;
  }

  public int timeoutSend() {
    return timeoutSend;
  }

  public int timeoutRecv() {
    return timeoutRecv;
  }

  public long reconnectInterval() {
    return reconnectInterval;
  }

  public long reconnectIntervalMax() {
    return reconnectIntervalMax;
  }
}
