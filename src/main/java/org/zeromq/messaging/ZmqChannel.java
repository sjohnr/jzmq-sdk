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
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.zeromq.support.ZmqUtils.makeHash;
import static org.zeromq.support.ZmqUtils.mapAsJson;

public final class ZmqChannel implements HasDestroy {

  private static final int INPROC_CONN_TIMEOUT = 1000; // inproc protocol conn timeout, best guess.

  private static final int POLLABLE_IND_NOT_INITIALIZED = -1;

  private static final Logger LOG = LoggerFactory.getLogger(ZmqChannel.class);

  public static final class Builder implements ObjectBuilder<ZmqChannel>, HasInvariant {

    private final ZmqChannel _target = new ZmqChannel();

    private Builder(int socketType, ZmqContext ctx) {
      _target.socketType = socketType;
      _target.ctx = ctx;
    }

    public Builder withProps(Props props) {
      _target.props = props;
      return this;
    }

    @Override
    public void checkInvariant() {
      if (_target.ctx == null) {
        throw ZmqException.fatal();
      }
      if (_target.props == null) {
        throw ZmqException.fatal();
      }
      switch (_target.socketType) {
        case ZMQ.PUB:
        case ZMQ.SUB:
        case ZMQ.XPUB:
        case ZMQ.XSUB:
        case ZMQ.PUSH:
        case ZMQ.PULL:
        case ZMQ.DEALER:
        case ZMQ.ROUTER:
          break;
        default:
          throw ZmqException.fatal();
      }
    }

    @Override
    public ZmqChannel build() {
      checkInvariant();

      if (_target.socketType == ZMQ.ROUTER) {
        _target._inputAdapter = InputMessageAdapter.builder().expectIdentities().build();
        _target._outputAdapter = OutputMessageAdapter.builder().expectIdentities().build();
      }
      if (_target.socketType == ZMQ.DEALER) {
        _target._inputAdapter = InputMessageAdapter.builder().expectIdentities().build();
        _target._outputAdapter = OutputMessageAdapter.builder().awareOfDEALERType().expectIdentities().build();
      }
      if (_target.socketType == ZMQ.PUB || _target.socketType == ZMQ.PUSH) {
        _target._inputAdapter = null;
        _target._outputAdapter = OutputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
      }
      if (_target.socketType == ZMQ.SUB || _target.socketType == ZMQ.PULL) {
        _target._outputAdapter = null;
        _target._inputAdapter = InputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
      }
      if (_target.socketType == ZMQ.XPUB) {
        _target._inputAdapter = InputMessageAdapter.builder().awareOfTopicFrame().awareOfExtendedPubSub().build();
        _target._outputAdapter = OutputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
      }
      if (_target.socketType == ZMQ.XSUB) {
        _target._inputAdapter = InputMessageAdapter.builder().awareOfTopicFrame().expectIdentities().build();
        _target._outputAdapter = OutputMessageAdapter.builder().awareOfTopicFrame().awareOfExtendedPubSub().build();
      }

      _target._socket = createSocket();

      return _target;
    }

    /** @return connected and/or bound {@link ZMQ.Socket} object. */
    ZMQ.Socket createSocket() {
      ZMQ.Socket socket = _target.ctx.newSocket(_target.socketType);

      {
        // set high water marks.
        socket.setSndHWM(_target.props.hwmSend());
        socket.setRcvHWM(_target.props.hwmRecv());

        // set socket .send()/.recv() timeout.
        socket.setSendTimeOut(_target.props.timeoutSend());
        socket.setReceiveTimeOut(_target.props.timeoutRecv());

        // set LINGER.
        socket.setLinger(_target.props.linger());

        // set socket identity.
        if (_target.props.identity() != null) {
          socket.setIdentity(_target.props.identity().getBytes());
        }

        // set reconnect settings.
        socket.setReconnectIVL(_target.props.reconnectInterval());
        socket.setReconnectIVLMax(_target.props.reconnectIntervalMax());
      }

      // ... bind().
      for (String addr : _target.props.bindAddr()) {
        try {
          socket.bind(addr);
        }
        catch (Exception e) {
          LOG.error("!!! Got error at .bind(" + addr + "): " + e, e);
          throw ZmqException.seeCause(e);
        }
      }

      // ... connect().
      for (String addr : _target.props.connectAddr()) {
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
      opts.put("bind_addr", _target.props.bindAddr());
      opts.put("connect_addr", _target.props.connectAddr());
      opts.put("timeout_send", socket.getSendTimeOut());
      opts.put("timeout_recv", socket.getReceiveTimeOut());
      opts.put("reconn_intrvl", socket.getReconnectIVL());
      opts.put("reconn_intrvl_max", socket.getReconnectIVLMax());

      LOG.info("Created socket: {}.", mapAsJson(opts));
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

  private ZmqContext ctx;
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

  public static Builder DEALER(ZmqContext ctx) {
    return new Builder(ZMQ.DEALER, ctx);
  }

  public static Builder ROUTER(ZmqContext ctx) {
    return new Builder(ZMQ.ROUTER, ctx);
  }

  public static Builder PUB(ZmqContext ctx) {
    return new Builder(ZMQ.PUB, ctx);
  }

  public static Builder SUB(ZmqContext ctx) {
    return new Builder(ZMQ.SUB, ctx);
  }

  public static Builder XPUB(ZmqContext ctx) {
    return new Builder(ZMQ.XPUB, ctx);
  }

  public static Builder XSUB(ZmqContext ctx) {
    return new Builder(ZMQ.XSUB, ctx);
  }

  public static Builder PUSH(ZmqContext ctx) {
    return new Builder(ZMQ.PUSH, ctx);
  }

  public static Builder PULL(ZmqContext ctx) {
    return new Builder(ZMQ.PULL, ctx);
  }

  @Override
  public void destroy() {
    unregister();
    ctx.closeSocket(_socket);
    _socket = null; // invalidates socket.
  }

  /**
   * Function which sends a message. May block if timeout on socket has been specified.
   *
   * @param message message to send.
   * @return flag indicating success or fail for send operation.
   */
  public boolean send(ZmqMessage message) {
    assertSocket();
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
    assertSocket();
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
    assertSocket();
    _socket.subscribe(topic);
  }

  /**
   * Unsubscribe function.
   *
   * @param topic the "topic" to unsubscribe from.
   */
  public void unsubscribe(byte[] topic) {
    assertSocket();
    _socket.unsubscribe(topic);
  }

  /**
   * Set XPUB_VERBOSE flag. Send duplicate subscriptions/unsubscriptions
   * on XPUB/SUB connection.
   * <p/>
   * <b>NOTE: this setting only makes sense on XPUB socket.</b>
   */
  public void setExtendedPubSubVerbose() {
    assertSocket();
    _socket.setXpubVerbose(true);
  }

  /**
   * Unset XPUB_VERBOSE flag. Send duplicate subscriptions/unsubscriptions
   * on XPUB/SUB connection.
   * <p/>
   * <b>NOTE: this setting only makes sense on XPUB socket.</b>
   */
  public void unsetExtendedPubSubVerbose() {
    assertSocket();
    _socket.setXpubVerbose(false);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSendRecv(ZMQ.Poller poller) {
    assertSocket();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT | ZMQ.Poller.POLLIN);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSend(ZMQ.Poller poller) {
    assertSocket();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchRecv(ZMQ.Poller poller) {
    assertSocket();
    if (isRegistered()) {
      throw ZmqException.fatal();
    }
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLIN);
  }

  /** Clears internal poller on internal {@link #_socket}. */
  public void unregister() {
    assertSocket();
    if (_poller != null) {
      _poller.unregister(_socket);
      _poller = null;
      _pollableInd = POLLABLE_IND_NOT_INITIALIZED;
    }
  }

  /** Determines whether internal {@link #_socket} is ready for reading message w/o blocking. */
  public boolean canRecv() {
    assertSocket();
    if (!isRegistered()) {
      throw ZmqException.fatal();
    }
    return _poller.pollin(_pollableInd);
  }

  /** Determines whether internal {@link #_socket} is ready for writing message w/o blocking. */
  public boolean canSend() {
    assertSocket();
    if (!isRegistered()) {
      throw ZmqException.fatal();
    }
    return _poller.pollout(_pollableInd);
  }

  private void assertSocket() {
    if (_socket == null) {
      throw ZmqException.fatal();
    }
  }

  private boolean isRegistered() {
    return _pollableInd != POLLABLE_IND_NOT_INITIALIZED;
  }
}
