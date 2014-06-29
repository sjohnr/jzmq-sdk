package org.zeromq.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zeromq.ZMQ.SNDMORE;
import static org.zeromq.messaging.ZmqFrames.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.makeHash;

public final class ZmqChannel implements HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqChannel.class);

  private static final int INPROC_CONN_TIMEOUT = 1000; // inproc protocol conn timeout, best guess.
  private static final int POLLABLE_IND_NOT_INITIALIZED = -1;

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
      checkArgument(_target.ctx != null);
      checkArgument(_target.props != null);

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
          throw new IllegalArgumentException("Wrong socketType=" + _target.socketType);
      }
    }

    @Override
    public ZmqChannel build() {
      checkInvariant();

      _target._payloadBuf = new byte[_target.props.payloadBufCapacity()];
      _target._inprocRefBuf = new byte[4/*integer*/];

      ZMQ.Socket socket = _target.ctx.newSocket(_target.socketType);

      {
        // set high water marks.
        socket.setSndHWM(_target.props.hwmSend());
        socket.setRcvHWM(_target.props.hwmRecv());

        // set socket .send()/.recv() timeout.
        socket.setSendTimeOut(_target.props.sendTimeout());
        socket.setReceiveTimeOut(_target.props.recvTimeout());

        // set LINGER.
        socket.setLinger(_target.props.linger());

        // set socket identity.
        if (_target.props.identity() != null) {
          socket.setIdentity(_target.props.identity());
        }

        // set ROUTER_MANDATORY flag.
        if (_target.socketType == ZMQ.ROUTER) {
          socket.setRouterMandatory(_target.props.isRouterMandatory());
        }
      }

      // ... bind().
      for (String addr : _target.props.bindAddr()) {
        try {
          socket.bind(addr);
        }
        catch (Exception e) {
          LOG.error("!!! Got error at .bind(addr=" + addr + "): " + e, e);
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
                LOG.error("!!! Can't .connect(addr=" + addr + ")." + " Gave up after " + timeout + " sec.");
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

      _target._socket = socket;

      return _target;
    }

    void logOpts(ZMQ.Socket socket) {
      Map<String, Object> opts = new LinkedHashMap<String, Object>();

      opts.put("type", getLoggableSocketType());
      opts.put("bind_addr", _target.props.bindAddr());
      opts.put("connect_addr", _target.props.connectAddr());
      opts.put("hwm_send", socket.getSndHWM());
      opts.put("hwm_recv", socket.getRcvHWM());
      opts.put("timeout_send", socket.getSendTimeOut());
      opts.put("timeout_recv", socket.getReceiveTimeOut());
      opts.put("custom_identity", makeHash(socket.getIdentity()));
      opts.put("reconn_intrvl", socket.getReconnectIVL());
      opts.put("reconn_intrvl_max", socket.getReconnectIVLMax());
      opts.put("linger", socket.getLinger());
      if (_target.socketType == ZMQ.ROUTER) {
        opts.put("router_mandatory", _target.props.isRouterMandatory());
      }
      opts.put("payload_buf_capacity", _target.props.payloadBufCapacity());

      String result;
      try {
        result = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(opts);
      }
      catch (JsonProcessingException e) {
        throw ZmqException.seeCause(e);
      }
      LOG.info("Created socket: {}.", result);
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
          throw new IllegalArgumentException("Wrong socketType=" + _target.socketType);
      }
      return loggableSocketType;
    }
  }

  private ZmqContext ctx;
  private int socketType = -1;
  private Props props;

  private ZMQ.Socket _socket;
  private ZMQ.Poller _poller;
  private int _pollableInd = POLLABLE_IND_NOT_INITIALIZED;
  private byte[] _payloadBuf;
  private byte[] _inprocRefBuf;

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
    _socket = null;
  }

  public boolean sendFrames(ZmqFrames frames, int flag) {
    checkState(_socket != null);
    int size = frames.size();
    int i = 0;
    boolean sent = false;
    for (byte[] frame : frames) {
      sent = _socket.send(frame, ++i < size ? SNDMORE : flag);
      if (!sent) {
        return false;
      }
    }
    return sent;
  }

  public boolean pub(byte[] topic, byte[] payload, int flag) {
    checkState(_socket != null);
    if (!_socket.send(topic, SNDMORE)) {
      return false;
    }
    int len = putPayload(payload);
    return _socket.send(_payloadBuf, 0, len, flag);
  }

  public boolean pubInprocRef(byte[] topic, int i, int flag) {
    checkState(_socket != null);
    if (!_socket.send(topic, SNDMORE)) {
      return false;
    }
    putInt(_inprocRefBuf, 0, i);
    return _socket.send(_inprocRefBuf, flag);
  }

  public boolean send(byte[] payload, int flag) {
    checkState(_socket != null);
    int len = putPayload(payload);
    return _socket.send(_payloadBuf, 0, len, flag);
  }

  public boolean sendInprocRef(int i, int flag) {
    checkState(_socket != null);
    putInt(_inprocRefBuf, 0, i);
    return _socket.send(_inprocRefBuf, flag);
  }

  public boolean route(ZmqFrames identities, byte[] payload, int flag) {
    checkState(_socket != null);
    putIdentities(identities);
    int len = putPayload(payload);
    return _socket.send(_payloadBuf, 0, len, flag);
  }

  public boolean routeInprocRef(ZmqFrames identities, int i, int flag) {
    checkState(_socket != null);
    putIdentities(identities);
    putInt(_inprocRefBuf, 0, i);
    return _socket.send(_inprocRefBuf, flag);
  }

  /**
   * Receives frames.
   *
   * @param flag block/dont block flag. See {@link ZMQ#DONTWAIT}, {@link ZMQ#NOBLOCK} and {@code 0}(for block).
   * @return frames or null.
   */
  public ZmqFrames recv(int flag) {
    ZmqFrames input = new ZmqFrames();
    for (; ; ) {
      byte[] frame = _socket.recv(flag);
      if (frame == null) {
        return null;
      }
      input.add(frame);
      if (!_socket.hasReceiveMore()) {
        break;
      }
    }
    return input;
  }

  /**
   * Subscribe on topic.
   *
   * @param topic the "topic" to subscribe on.
   */
  public void subscribe(byte[] topic) {
    checkState(_socket != null);
    _socket.subscribe(topic);
  }

  /**
   * Unsubscribe from topic.
   *
   * @param topic the "topic" to unsubscribe from.
   */
  public void unsubscribe(byte[] topic) {
    checkState(_socket != null);
    _socket.unsubscribe(topic);
  }

  /**
   * Set XPUB_VERBOSE flag. Send duplicate subscriptions/unsubscriptions
   * on XPUB/SUB connection.
   * <p/>
   * <b>NOTE: this setting only makes sense on XPUB socket.</b>
   */
  public void setExtendedPubSubVerbose() {
    checkState(_socket != null);
    _socket.setXpubVerbose(true);
  }

  /**
   * Unset XPUB_VERBOSE flag. Send duplicate subscriptions/unsubscriptions
   * on XPUB/SUB connection.
   * <p/>
   * <b>NOTE: this setting only makes sense on XPUB socket.</b>
   */
  public void unsetExtendedPubSubVerbose() {
    checkState(_socket != null);
    _socket.setXpubVerbose(false);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSendRecv(ZMQ.Poller poller) {
    checkState(_socket != null);
    checkState(!isRegistered());
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT | ZMQ.Poller.POLLIN);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchSend(ZMQ.Poller poller) {
    checkState(_socket != null);
    checkState(!isRegistered());
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLOUT);
  }

  /** Registers internal {@link #_socket} on given poller instance. */
  public void watchRecv(ZMQ.Poller poller) {
    checkState(_socket != null);
    checkState(!isRegistered());
    _poller = poller;
    _pollableInd = _poller.register(_socket, ZMQ.Poller.POLLIN);
  }

  /** Clears internal poller on internal {@link #_socket}. */
  public void unregister() {
    checkState(_socket != null);
    if (_poller != null) {
      _poller.unregister(_socket);
      _poller = null;
      _pollableInd = POLLABLE_IND_NOT_INITIALIZED;
    }
  }

  /** Determines whether internal {@link #_socket} is ready for reading message w/o blocking. */
  public boolean canRecv() {
    checkState(_socket != null);
    checkState(isRegistered());
    return _poller.pollin(_pollableInd);
  }

  /** Determines whether internal {@link #_socket} is ready for writing message w/o blocking. */
  public boolean canSend() {
    checkState(_socket != null);
    checkState(isRegistered());
    return _poller.pollout(_pollableInd);
  }

  private boolean isRegistered() {
    return _pollableInd != POLLABLE_IND_NOT_INITIALIZED;
  }

  private void putIdentities(ZmqFrames identities) {
    if (socketType == ZMQ.DEALER) {
      _socket.send(EMPTY_FRAME, SNDMORE);
    }
    for (byte[] frame : identities) {
      _socket.send(frame, SNDMORE);
      _socket.send(EMPTY_FRAME, SNDMORE);
    }
    _socket.send(EMPTY_FRAME, SNDMORE);
  }

  private int putPayload(byte[] payload) {
    putInt(_payloadBuf, 0, payload.length);
    System.arraycopy(payload, 0, _payloadBuf, 4, payload.length);
    return 4 + payload.length;
  }

  private void putInt(byte[] buf, int offset, int i) {
    buf[offset] = (byte) (i >> 24);
    buf[++offset] = (byte) (i >> 16);
    buf[++offset] = (byte) (i >> 8);
    buf[++offset] = (byte) i;
  }
}
