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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.messaging.event.ZmqChannelEventWrapper;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.ZmqUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class ZmqChannelFactory {

  private static final int INPROC_CONN_TIMEOUT = 1000; // inproc protocol conn timeout, best guess.

  private static final int DEFAULT_LINGER = 0; // by default it's allowed to close socket and not be blocked.
  private static final int DEFAULT_WAIT_ON_SEND = 1000; // how long to wait on .send(), best guess.
  private static final int DEFAULT_WAIT_ON_RECV = 1000; // how long to wait on .recv(), best guess.
  private static final long DEFAULT_HWM_SEND = 1000; // HWM, best guess.
  private static final long DEFAULT_HWM_RECV = 1000; // HWM, best guess.

  private static final Logger LOG = LoggerFactory.getLogger(ZmqChannelFactory.class);

  /** Class which represents the only correct way to create {@link ZmqChannel}-s. */
  public static final class Builder implements ObjectBuilder<ZmqChannelFactory> {

    private final ZmqChannelFactory _target = new ZmqChannelFactory();

    private Builder() {
    }

    public Builder withEventListener(Object el) {
      _target.eventListeners.add(el);
      return this;
    }

    public Builder withEventListeners(Iterable eventListeners) {
      for (Object el : eventListeners) {
        withEventListener(el);
      }
      return this;
    }

    public Builder withZmqContext(ZmqContext zmqContext) {
      _target.zmqContext = zmqContext;
      return this;
    }

    public Builder ofDEALERType() {
      _target.socketType = ZMQ.DEALER;
      return this;
    }

    public Builder ofROUTERType() {
      _target.socketType = ZMQ.ROUTER;
      return this;
    }

    public Builder ofPUBType() {
      _target.socketType = ZMQ.PUB;
      return this;
    }

    public Builder ofSUBType() {
      _target.socketType = ZMQ.SUB;
      return this;
    }

    public Builder ofPUSHType() {
      _target.socketType = ZMQ.PUSH;
      return this;
    }

    public Builder ofPULLType() {
      _target.socketType = ZMQ.PULL;
      return this;
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

    public Builder withConnectAddress(String address) {
      _target.connectAddresses.add(address);
      return this;
    }

    public Builder withConnectAddresses(Iterable<String> addresses) {
      for (String address : addresses) {
        withConnectAddress(address);
      }
      return this;
    }

    public Builder withHwmForSend(long hwmForSend) {
      if (hwmForSend > 0) {
        _target.hwmSend = hwmForSend;
      }
      return this;
    }

    public Builder withHwmForRecv(long hwmForRecv) {
      if (hwmForRecv > 0) {
        _target.hwmRecv = hwmForRecv;
      }
      return this;
    }

    public Builder withSocketIdentityPrefix(byte[] socketIdentityPrefix) {
      _target.socketIdentityPrefix = socketIdentityPrefix;
      return this;
    }

    public Builder withLinger(long linger) {
      if (linger >= 0) {
        _target.linger = linger;
      }
      return this;
    }

    public Builder withWaitOnSend(int timeout) {
      if (timeout > 0) {
        _target.timeoutSend = timeout;
      }
      return this;
    }

    public Builder withBlockOnSend() {
      _target.timeoutSend = -1;
      return this;
    }

    public Builder withWaitOnRecv(int timeout) {
      if (timeout > 0) {
        _target.timeoutRecv = timeout;
      }
      return this;
    }

    public Builder withBlockOnRecv() {
      _target.timeoutRecv = -1;
      return this;
    }

    @Override
    public void checkInvariant() {
      _target.checkInvariant();
    }

    @Override
    public ZmqChannelFactory build() {
      checkInvariant();
      _target.build();
      return _target;
    }
  }

  /** SPI class. Default (and the only) {@link ZmqChannel} implementation. */
  private static class ChannelImpl implements ZmqChannel {

    static final int POLLABLE_IND_NOT_INITIALIZED = -1;

    static enum Mode {
      BOTH,
      SENDER,
      RECEIVER
    }

    final ZmqContext zmqContext;
    ZMQ.Socket socket;
    final Mode mode;
    final ObjectAdapter<ZmqFrames, ZmqMessage> inputAdapter;
    final ObjectAdapter<ZmqMessage, ZmqFrames> outputAdapter;

    ZMQ.Poller _pollerOnReceiver;
    ZMQ.Poller _pollerOnSender;
    int _pollableIndOnReceiver = POLLABLE_IND_NOT_INITIALIZED;
    int _pollableIndOnSender = POLLABLE_IND_NOT_INITIALIZED;

    ChannelImpl(ZmqContext zmqContext,
                ZMQ.Socket socket,
                Mode mode,
                ObjectAdapter<ZmqFrames, ZmqMessage> inputAdapter,
                ObjectAdapter<ZmqMessage, ZmqFrames> outputAdapter) {
      this.zmqContext = zmqContext;
      this.socket = socket;
      this.mode = mode;
      this.inputAdapter = inputAdapter;
      this.outputAdapter = outputAdapter;
    }

    @Override
    public void register(ZMQ.Poller poller) {
      assertSocketAlive();
      if (mode == Mode.BOTH || mode == Mode.RECEIVER) {
        registerReceiver(poller);
      }
      if (mode == Mode.BOTH || mode == Mode.SENDER) {
        registerSender();
      }
    }

    @Override
    public void unregister() {
      assertSocketAlive();
      unregisterReceiver();
      unregisterSender();
    }

    @Override
    public boolean hasInput() {
      assertSocketAlive();
      if (mode == Mode.BOTH || mode == Mode.RECEIVER) {
        if (!isReceiverRegistered()) {
          registerReceiver(zmqContext.newPoller(1));
        }
        return _pollerOnReceiver.pollin(_pollableIndOnReceiver);
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean hasOutput() {
      assertSocketAlive();
      if (mode == Mode.BOTH || mode == Mode.SENDER) {
        if (!isSenderRegistered()) {
          registerSender();
        }
        return _pollerOnSender.pollout(_pollableIndOnSender);
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean send(ZmqMessage message) throws ZmqException {
      assertSocketAlive();
      if (mode == Mode.BOTH || mode == Mode.SENDER) {
        try {
          ZmqFrames output = outputAdapter.convert(message);
          int outputSize = output.size();
          int i = 0;
          boolean sendWasGood = true;
          for (byte[] frame : output) {
            sendWasGood &= socket.send(frame, ++i < outputSize ? ZMQ.SNDMORE : 0);
            if (!sendWasGood) {
              return false;
            }
          }
          return sendWasGood;
        }
        catch (Exception e) {
          LOG.error("!!! Message wasn't sent! Exception occured: " + e, e);
          throw ZmqException.wrap(e);
        }
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public ZmqMessage recv() throws ZmqException {
      assertSocketAlive();
      if (mode == Mode.BOTH || mode == Mode.RECEIVER) {
        try {
          ZmqFrames input = new ZmqFrames();
          for (; ; ) {
            byte[] frame = socket.recv(0);
            if (frame == null) {
              return null;
            }
            input.add(frame);
            if (!socket.hasReceiveMore()) {
              break;
            }
          }
          return inputAdapter.convert(input);
        }
        catch (Exception e) {
          LOG.error("!!! Message wasn't received! Exception occured: " + e, e);
          throw ZmqException.wrap(e);
        }
      }
      throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(byte[] topic) {
      assertSocketAlive();
      if (mode == Mode.RECEIVER) {
        socket.subscribe(topic);
      }
    }

    @Override
    public void unsubscribe(byte[] topic) {
      assertSocketAlive();
      if (mode == Mode.RECEIVER) {
        socket.unsubscribe(topic);
      }
    }

    @Override
    public void destroy() {
      unregister();
      zmqContext.closeSocket(socket);
      socket = null; // invalidates socket.
    }

    void registerReceiver(ZMQ.Poller poller) {
      assert !isReceiverRegistered() : "not allowed resgistering channel(r) more than once!";
      _pollerOnReceiver = poller;
      _pollableIndOnReceiver = _pollerOnReceiver.register(socket, ZMQ.Poller.POLLIN);
    }

    boolean isReceiverRegistered() {
      return _pollableIndOnReceiver != POLLABLE_IND_NOT_INITIALIZED;
    }

    void registerSender() {
      assert !isSenderRegistered() : "not allowed resgistering channel(s) more than once!";
      _pollerOnSender = zmqContext.newPoller(1);
      _pollableIndOnSender = _pollerOnSender.register(socket, ZMQ.Poller.POLLOUT);
    }

    boolean isSenderRegistered() {
      return _pollableIndOnSender != POLLABLE_IND_NOT_INITIALIZED;
    }

    void unregisterReceiver() {
      if (_pollerOnReceiver != null) {
        _pollerOnReceiver.unregister(socket);
        _pollerOnReceiver = null;
        _pollableIndOnReceiver = POLLABLE_IND_NOT_INITIALIZED;
      }
    }

    void unregisterSender() {
      if (_pollerOnSender != null) {
        _pollerOnSender.unregister(socket);
        _pollerOnSender = null;
        _pollableIndOnSender = POLLABLE_IND_NOT_INITIALIZED;
      }
    }

    void assertSocketAlive() {
      assert socket != null : "detected access to the destroyed channel!";
    }
  }

  private ZmqContext zmqContext;
  private int socketType = -1;
  private List<String> bindAddresses = new ArrayList<String>();
  private List<String> connectAddresses = new ArrayList<String>();
  private long hwmSend = DEFAULT_HWM_SEND;
  private long hwmRecv = DEFAULT_HWM_RECV;
  private byte[] socketIdentityPrefix;
  private long linger = DEFAULT_LINGER;
  private int timeoutSend = DEFAULT_WAIT_ON_SEND;
  private int timeoutRecv = DEFAULT_WAIT_ON_RECV;
  private List<Object> eventListeners = new ArrayList<Object>();

  private ObjectAdapter<ZmqFrames, ZmqMessage> _inputAdapter;
  private ObjectAdapter<ZmqMessage, ZmqFrames> _outputAdapter;
  private ChannelImpl.Mode _mode;

  //// CONSTRUCTORS

  private ZmqChannelFactory() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public ZmqChannel newChannel() {
    if (_mode == null) {
      checkInvariant();
      build();
    }
    ZmqChannel channel = new ChannelImpl(zmqContext,
                                         newSocket(),
                                         _mode,
                                         _inputAdapter,
                                         _outputAdapter);
    if (!eventListeners.isEmpty()) {
      channel = new ZmqChannelEventWrapper(channel, eventListeners);
    }
    return channel;
  }

  private void checkInvariant() {
    assert zmqContext != null : "context is required!";

    if (bindAddresses.isEmpty() && connectAddresses.isEmpty()) {
      throw new AssertionError("either bind_addresses or connect_addresses must be specified! (or both)");
    }

    switch (socketType) {
      case ZMQ.PUB:
      case ZMQ.SUB:
      case ZMQ.PUSH:
      case ZMQ.PULL:
      case ZMQ.DEALER:
      case ZMQ.ROUTER:
        break;
      default:
        throw new AssertionError("Unsupported socket_type: " + socketType);
    }
  }

  private void build() {
    if (socketType == ZMQ.ROUTER) {
      _mode = ChannelImpl.Mode.BOTH;
      _inputAdapter = InputMessageAdapter.builder().expectIdentities().build();
      _outputAdapter = OutputMessageAdapter.builder().expectIdentities().build();
    }

    if (socketType == ZMQ.DEALER) {
      _mode = ChannelImpl.Mode.BOTH;
      _inputAdapter = InputMessageAdapter.builder().expectIdentities().build();
      _outputAdapter = OutputMessageAdapter.builder().awareOfDEALERType().expectIdentities().build();
    }

    if (socketType == ZMQ.PUB || socketType == ZMQ.PUSH) {
      _mode = ChannelImpl.Mode.SENDER;
      _inputAdapter = null;
      _outputAdapter = OutputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
    }

    if (socketType == ZMQ.SUB || socketType == ZMQ.PULL) {
      _mode = ChannelImpl.Mode.RECEIVER;
      _outputAdapter = null;
      _inputAdapter = InputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
    }
  }

  private ZMQ.Socket newSocket() {
    ZMQ.Socket socket = zmqContext.newSocket(socketType);

    {
      // set high water marks.
      socket.setSndHWM(hwmSend);
      socket.setRcvHWM(hwmRecv);

      // set socket .send()/.recv() timeout.
      socket.setSendTimeOut(timeoutSend);
      socket.setReceiveTimeOut(timeoutRecv);

      // setting LINGER to zero -- don't block on socket.close().
      socket.setLinger(linger);

      // set socket identity: provided socket_prefix plus UUID long.
      if (socketIdentityPrefix != null) {
        byte[] uuid = ZmqUtils.longAsBytes(UUID.randomUUID().getLeastSignificantBits());
        byte[] mergedIdentity = ZmqUtils.mergeBytes(Arrays.asList(socketIdentityPrefix, uuid));
        socket.setIdentity(mergedIdentity);
      }
    }

    // ... and bind().
    for (String addr : bindAddresses) {
      try {
        socket.bind(addr);
      }
      catch (Exception e) {
        LOG.error("!!! Got error at .bind(" + addr + "): " + e, e);
        throw ZmqException.wrap(e);
      }
    }

    // ... and connect().
    for (String addr : connectAddresses) {
      // check if this is inproc: address.
      if (addr.startsWith("inproc://")) {
        long timer = System.currentTimeMillis();
        for (; ; ) {
          try {
            socket.connect(addr);
            break;
          }
          catch (Exception e) {
            int timeout = INPROC_CONN_TIMEOUT;
            if (System.currentTimeMillis() - timer > timeout) {
              LOG.error("!!! Can't .connect(" + addr + ")." + " Gave up after " + timeout + " sec.");
              throw ZmqException.wrap(e);
            }
          }
        }
      }
      else {
        try {
          socket.connect(addr);
        }
        catch (Exception e) {
          LOG.error("!!! Got error at .connect(" + addr + "): " + e, e);
          throw ZmqException.wrap(e);
        }
      }
    }

    Map<String, Object> opts = new LinkedHashMap<String, Object>();

    if (socketIdentityPrefix != null) {
      opts.put("identity", ZmqUtils.makeHash(ImmutableList.of(socket.getIdentity())));
    }
    opts.put("type", getLoggableSocketType());
    opts.put("hwm_send", socket.getSndHWM());
    opts.put("hwm_recv", socket.getRcvHWM());
    opts.put("linger", socket.getLinger());
    opts.put("bind_addr", bindAddresses);
    opts.put("connect_addr", connectAddresses);
    opts.put("timeout_send", socket.getSendTimeOut());
    opts.put("timeout_recv", socket.getReceiveTimeOut());

    LOG.info("Created socket: {}.", ZmqUtils.mapAsJson(opts));
    return socket;
  }

  private String getLoggableSocketType() {
    String loggableSocketType;
    switch (socketType) {
      case ZMQ.PUB:
        loggableSocketType = "PUB";
        break;
      case ZMQ.SUB:
        loggableSocketType = "SUB";
        break;
      case ZMQ.PUSH:
        loggableSocketType = "PUSH";
        break;
      case ZMQ.PULL:
        loggableSocketType = "PULL";
        break;
      case ZMQ.DEALER:
        loggableSocketType = "DEALER";
        break;
      case ZMQ.ROUTER:
        loggableSocketType = "ROUTER";
        break;
      default:
        throw new IllegalArgumentException("Unsupported socket_type: " + socketType);
    }
    return loggableSocketType;
  }
}
