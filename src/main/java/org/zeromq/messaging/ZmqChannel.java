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
import org.zeromq.support.HasDestroy;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.zeromq.support.ZmqUtils.longAsBytes;
import static org.zeromq.support.ZmqUtils.makeHash;
import static org.zeromq.support.ZmqUtils.mapAsJson;
import static org.zeromq.support.ZmqUtils.mergeBytes;

public final class ZmqChannel implements HasDestroy {

  private static final int INPROC_CONN_TIMEOUT = 1000; // inproc protocol conn timeout, best guess.

  private static final int POLLABLE_IND_NOT_INITIALIZED = -1;

  private static final Logger LOG = LoggerFactory.getLogger(ZmqChannel.class);

  public static final class Builder implements ObjectBuilder<ZmqChannel> {

    private final ZmqChannel _target = new ZmqChannel();

    private Builder() {
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

    public Builder ofXPUBType() {
      _target.socketType = ZMQ.XPUB;
      return this;
    }

    public Builder ofXSUBType() {
      _target.socketType = ZMQ.XSUB;
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

    public Builder withProps(Props props) {
      _target.props = props;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.zmqContext == null) {
        throw ZmqException.fatal();
      }
      switch (_target.socketType) {
        case ZMQ.PUB:
        case ZMQ.SUB:
        case ZMQ.PUSH:
        case ZMQ.PULL:
        case ZMQ.DEALER:
        case ZMQ.ROUTER:
          break;
        default:
          throw ZmqException.fatal();
      }
      if (_target.props == null) {
        throw ZmqException.fatal();
      }
    }

    @Override
    public ZmqChannel build() {
      checkInvariant();

      if (_target.socketType == ZMQ.ROUTER) {
        _target._inputAdapter = InputMessageAdapter.builder()
                                                   .expectIdentities()
                                                   .build();
        _target._outputAdapter = OutputMessageAdapter.builder()
                                                     .expectIdentities()
                                                     .build();
      }

      if (_target.socketType == ZMQ.DEALER) {
        _target._inputAdapter = InputMessageAdapter.builder()
                                                   .expectIdentities()
                                                   .build();
        _target._outputAdapter = OutputMessageAdapter.builder()
                                                     .awareOfDEALERType()
                                                     .expectIdentities()
                                                     .build();
      }

      if (_target.socketType == ZMQ.PUB || _target.socketType == ZMQ.XPUB || _target.socketType == ZMQ.PUSH) {
        _target._inputAdapter = null;
        _target._outputAdapter = OutputMessageAdapter.builder()
                                                     .awareOfTopicFrame()
                                                     .expectIdentities()
                                                     .build();
      }

      if (_target.socketType == ZMQ.SUB || _target.socketType == ZMQ.XSUB || _target.socketType == ZMQ.PULL) {
        _target._outputAdapter = null;
        _target._inputAdapter = InputMessageAdapter.builder()
                                                   .awareOfTopicFrame()
                                                   .expectIdentities()
                                                   .build();
      }

      _target._socket = newSocket();

      return _target;
    }

    /** @return connected and/or bound {@link ZMQ.Socket} object. */
    ZMQ.Socket newSocket() {
      ZMQ.Socket socket = _target.zmqContext.newSocket(_target.socketType);

      {
        // set high water marks.
        socket.setSndHWM(_target.props.getHwmSend());
        socket.setRcvHWM(_target.props.getHwmRecv());

        // set socket .send()/.recv() timeout.
        socket.setSendTimeOut(_target.props.getTimeoutSend());
        socket.setReceiveTimeOut(_target.props.getTimeoutRecv());

        // setting LINGER to zero -- don't block on socket.close().
        socket.setLinger(_target.props.getLinger());

        // set socket identity: provided socket_prefix plus UUID long.
        setupIdentity(socket);

        // set reconnect settings.
        socket.setReconnectIVL(_target.props.getReconnectInterval());
        socket.setReconnectIVLMax(_target.props.getReconnectIntervalMax());
      }

      // ... bind().
      for (String addr : _target.props.getBindAddresses()) {
        try {
          socket.bind(addr);
        }
        catch (Exception e) {
          LOG.error("!!! Got error at .bind(" + addr + "): " + e, e);
          throw ZmqException.seeCause(e);
        }
      }

      // ... connect().
      for (String addr : _target.props.getConnectAddresses()) {
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
                throw ZmqException.seeCause(e);
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
            throw ZmqException.seeCause(e);
          }
        }
      }

      logOpts(socket);

      return socket;
    }

    void logOpts(ZMQ.Socket socket) {
      Map<String, Object> opts = new LinkedHashMap<String, Object>();

      opts.put("type", getLoggableSocketType());
      opts.put("identity", makeHash(ImmutableList.of(socket.getIdentity())));
      opts.put("hwm_send", socket.getSndHWM());
      opts.put("hwm_recv", socket.getRcvHWM());
      opts.put("linger", socket.getLinger());
      opts.put("bind_addr", _target.props.getBindAddresses());
      opts.put("connect_addr", _target.props.getConnectAddresses());
      opts.put("timeout_send", socket.getSendTimeOut());
      opts.put("timeout_recv", socket.getReceiveTimeOut());
      opts.put("reconn_intrvl", socket.getReconnectIVL());
      opts.put("reconn_intrvl_max", socket.getReconnectIVLMax());

      LOG.info("Created socket: {}.", mapAsJson(opts));
    }

    void setupIdentity(ZMQ.Socket socket) {
      if (_target.props.getSocketIdPrefix() != null) {
        byte[] delimiter = "#".getBytes();
        byte[] uuid = longAsBytes(UUID.randomUUID().getMostSignificantBits());
        byte[] identity = mergeBytes(ImmutableList.of(_target.props.getSocketIdPrefix().getBytes(), delimiter, uuid));
        socket.setIdentity(identity);
      }
    }

    String getLoggableSocketType() {
      String loggableSocketType;
      switch (_target.socketType) {
        case ZMQ.PUB:
          loggableSocketType = "PUB";
          break;
        case ZMQ.SUB:
          loggableSocketType = "SUB";
          break;
        case ZMQ.XPUB:
          loggableSocketType = "XPUB";
          break;
        case ZMQ.XSUB:
          loggableSocketType = "XSUB";
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
          throw ZmqException.fatal();
      }
      return loggableSocketType;
    }
  }

  private ZmqContext zmqContext;
  private int socketType = -1;
  private Props props;

  private ZMQ.Socket _socket;
  private ObjectAdapter<ZmqFrames, ZmqMessage> _inputAdapter;
  private ObjectAdapter<ZmqMessage, ZmqFrames> _outputAdapter;
  private ZMQ.Poller _poller;
  private int _pollableInd = POLLABLE_IND_NOT_INITIALIZED;

  //// CONSTRUCTORS

  protected ZmqChannel() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void destroy() {
    unregister();
    zmqContext.closeSocket(_socket);
    _socket = null; // invalidates socket.
  }

  /**
   * Function which sends a message. May block if timeout on socket has been specified.
   *
   * @param message message to send.
   * @return flag indicating success or fail for send operation.
   */
  public boolean send(ZmqMessage message) {
    assertSocketAlive();
    try {
      ZmqFrames output = _outputAdapter.convert(message);
      int outputSize = output.size();
      int i = 0;
      boolean sent = false;
      for (byte[] frame : output) {
        sent = _socket.send(frame, ++i < outputSize ? ZMQ.SNDMORE : 0);
        if (!sent) {
          return false;
        }
      }
      return sent;
    }
    catch (Exception e) {
      LOG.error("!!! Message wasn't sent! Exception occured: " + e, e);
      throw ZmqException.seeCause(e);
    }
  }

  /**
   * Function which receives a message. May block if timeout on socket has been specified.
   *
   * @return a message or null.
   */
  public ZmqMessage recv() {
    assertSocketAlive();
    try {
      ZmqFrames input = new ZmqFrames();
      for (; ; ) {
        byte[] frame = _socket.recv(0);
        if (frame == null) {
          return null;
        }
        input.add(frame);
        if (!_socket.hasReceiveMore()) {
          break;
        }
      }
      return _inputAdapter.convert(input);
    }
    catch (Exception e) {
      LOG.error("!!! Message wasn't received! Exception occured: " + e, e);
      throw ZmqException.seeCause(e);
    }
  }

  /**
   * Subscribe function.
   *
   * @param topic the "topic" to subscribe on.
   */
  public void subscribe(byte[] topic) {
    assertSocketAlive();
    _socket.subscribe(topic);
  }

  /**
   * Unsubscribe function.
   *
   * @param topic the "topic" to unsubscribe from.
   */
  public void unsubscribe(byte[] topic) {
    assertSocketAlive();
    _socket.unsubscribe(topic);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSendRecv(ZMQ.Poller poller) {
    assertSocketAlive();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT | ZMQ.Poller.POLLIN);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSend(ZMQ.Poller poller) {
    assertSocketAlive();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchRecv(ZMQ.Poller poller) {
    assertSocketAlive();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLIN);
  }

  /** Clears internal poller on internal {@link #_socket}. */
  public void unregister() {
    assertSocketAlive();
    if (_poller != null) {
      _poller.unregister(_socket);
      _poller = null;
      _pollableInd = POLLABLE_IND_NOT_INITIALIZED;
    }
  }

  /** Determines whether internal {@link #_socket} is ready for reading message w/o blocking. */
  public boolean canRecv() {
    assertSocketAlive();
    if (!isRegistered()) {
      throw ZmqException.fatal();
    }
    return _poller.pollin(_pollableInd);
  }

  /** Determines whether internal {@link #_socket} is ready for writing message w/o blocking. */
  public boolean canSend() {
    assertSocketAlive();
    if (!isRegistered()) {
      throw ZmqException.fatal();
    }
    return _poller.pollout(_pollableInd);
  }

  private void assertSocketAlive() {
    if (_socket == null) {
      throw ZmqException.fatal();
    }
  }

  private boolean isRegistered() {
    return _pollableInd != POLLABLE_IND_NOT_INITIALIZED;
  }
}
