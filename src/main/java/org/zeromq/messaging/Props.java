package org.zeromq.messaging;

import com.google.common.base.Strings;
import org.zeromq.support.ObjectBuilder;

import java.util.ArrayList;
import java.util.List;

public final class Props {

  private static final int DEFAULT_LINGER = 0; // by default it's allowed to close socket and not be blocked.
  private static final int DEFAULT_SEND_TIMEOUT = 1000; // how long to wait on .send(), best guess.
  private static final int DEFAULT_RECV_TIMEOUT = 1000; // how long to wait on .recv(), best guess.
  private static final long DEFAULT_HWM_SEND = 1000; // HWM, best guess.
  private static final long DEFAULT_HWM_RECV = 1000; // HWM, best guess.
  private static final int DEFAULT_PROC_LIMIT = 1000;
  private static final int DEFAULT_CHUNK_BUF_CAPACITY = 8192; // buffer capacity for content, best guess.

  public static final class Builder implements ObjectBuilder<Props> {

    private final Props _target = new Props();

    private Builder() {
    }

    public Builder withBindAddr(String address) {
      _target.setBindAddr(address);
      return this;
    }

    public Builder withBindAddr(Iterable<String> addresses) {
      for (String address : addresses) {
        withBindAddr(address);
      }
      return this;
    }

    public Builder withConnectAddr(String address) {
      _target.setConnectAddr(address);
      return this;
    }

    public Builder withConnectAddr(Iterable<String> addresses) {
      for (String address : addresses) {
        withConnectAddr(address);
      }
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

    public Builder withIdentity(String identity) {
      _target.setIdentity(identity);
      return this;
    }

    public Builder withLinger(long linger) {
      _target.setLinger(linger);
      return this;
    }

    public Builder withSendTimeout(int timeout) {
      _target.setSendTimeout(timeout);
      return this;
    }

    public Builder withRecvTimeout(int timeout) {
      _target.setRecvTimeout(timeout);
      return this;
    }

    public Builder withRouterMandatory() {
      _target.setRouterMandatory(true);
      return this;
    }

    public Builder withRouterNotMandatory() {
      _target.setRouterMandatory(false);
      return this;
    }

    public final Builder withProcLimit(int procLimit) {
      _target.setProcLimit(procLimit);
      return this;
    }

    public final Builder withChunkBufCapacity(int contentBufCapacity) {
      _target.setChunkBufCapacity(contentBufCapacity);
      return this;
    }

    @Override
    public Props build() {
      return _target;
    }
  }

  private List<String> bindAddr = new ArrayList<String>();
  private List<String> connectAddr = new ArrayList<String>();
  private long hwmSend = DEFAULT_HWM_SEND;
  private long hwmRecv = DEFAULT_HWM_RECV;
  private String identity;
  private long linger = DEFAULT_LINGER;
  private int sendTimeout = DEFAULT_SEND_TIMEOUT;
  private int recvTimeout = DEFAULT_RECV_TIMEOUT;
  private boolean routerMandatory;
  private int procLimit = DEFAULT_PROC_LIMIT;
  private int chunkBufCapacity = DEFAULT_CHUNK_BUF_CAPACITY;

  //// CONSTRUCTORS

  private Props() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public void setBindAddr(String bindAddr) {
    for (String addr : bindAddr.split("[\\s\\t\\n,]+")) {
      addr = addr.trim();
      if (!Strings.isNullOrEmpty(addr)) {
        this.bindAddr.add(addr);
      }
    }
  }

  public void setConnectAddr(String connectAddr) {
    for (String addr : connectAddr.split("[\\s\\t\\n,]+")) {
      addr = addr.trim();
      if (!Strings.isNullOrEmpty(addr)) {
        this.connectAddr.add(addr);
      }
    }
  }

  public void setHwmSend(long hwmSend) {
    this.hwmSend = hwmSend;
  }

  public void setHwmRecv(long hwmRecv) {
    this.hwmRecv = hwmRecv;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public void setLinger(long linger) {
    this.linger = linger;
  }

  public void setSendTimeout(int timeout) {
    this.sendTimeout = timeout;
  }

  public void setRecvTimeout(int timeout) {
    this.recvTimeout = timeout;
  }

  public List<String> bindAddr() {
    return bindAddr;
  }

  public List<String> connectAddr() {
    return connectAddr;
  }

  public long hwmSend() {
    return hwmSend;
  }

  public long hwmRecv() {
    return hwmRecv;
  }

  public String identity() {
    return identity;
  }

  public long linger() {
    return linger;
  }

  public int sendTimeout() {
    return sendTimeout;
  }

  public int recvTimeout() {
    return recvTimeout;
  }

  public boolean isRouterMandatory() {
    return routerMandatory;
  }

  public void setRouterMandatory(boolean routerMandatory) {
    this.routerMandatory = routerMandatory;
  }

  public void setProcLimit(int procLimit) {
    this.procLimit = procLimit;
  }

  public int procLimit() {
    return procLimit;
  }

  public void setChunkBufCapacity(int chunkBufCapacity) {
    this.chunkBufCapacity = chunkBufCapacity;
  }

  public int chunkBufCapacity() {
    return chunkBufCapacity;
  }
}
