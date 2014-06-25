package org.zeromq.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.zeromq.ZMQ.SNDMORE;
import static org.zeromq.support.ZmqUtils.EMPTY_FRAME;
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

      _target._chunkBuf = new byte[_target.props.chunkBufCapacity()];
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
          socket.setIdentity(_target.props.identity().getBytes());
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
      opts.put("custom_identity", makeHash(ImmutableList.of(socket.getIdentity())));
      opts.put("hwm_send", socket.getSndHWM());
      opts.put("hwm_recv", socket.getRcvHWM());
      opts.put("linger", socket.getLinger());
      opts.put("bind_addr", _target.props.bindAddr());
      opts.put("connect_addr", _target.props.connectAddr());
      opts.put("timeout_send", socket.getSendTimeOut());
      opts.put("timeout_recv", socket.getReceiveTimeOut());
      opts.put("reconn_intrvl", socket.getReconnectIVL());
      opts.put("reconn_intrvl_max", socket.getReconnectIVLMax());
      if (_target.socketType == ZMQ.ROUTER) {
        opts.put("router_mandatory", _target.props.isRouterMandatory());
      }
      opts.put("proc_limit", _target.props.procLimit());
      opts.put("chunk_buf_capacity", _target.props.chunkBufCapacity());

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
          throw ZmqException.fatal();
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
  private byte[] _chunkBuf;
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
    _socket = null; // invalidates socket.
  }

  public boolean sendFrames(ZmqFrames frames, int flag) {
    assertSocket();
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

  public boolean pub(byte[] topic, byte[] headers, byte[] payload, int flag) {
    assertSocket();
    if (!_socket.send(topic, SNDMORE)) {
      return false;
    }
    int len = putContent(headers, payload);
    return _socket.send(_chunkBuf, 0, len, flag);
  }

  public boolean pubInprocRef(byte[] topic, int i, int flag) {
    assertSocket();
    if (!_socket.send(topic, SNDMORE)) {
      return false;
    }
    putInt(_inprocRefBuf, 0, i);
    return _socket.send(_inprocRefBuf, flag);
  }

  public boolean send(byte[] headers, byte[] payload, int flag) {
    assertSocket();
    int len = putContent(headers, payload);
    return _socket.send(_chunkBuf, 0, len, flag);
  }

  public boolean sendInprocRef(int i, int flag) {
    assertSocket();
    putInt(_inprocRefBuf, 0, i);
    return _socket.send(_inprocRefBuf, flag);
  }

  public boolean route(ZmqFrames identities, byte[] headers, byte[] payload, int flag) {
    assertSocket();
    putIdentities(identities);
    int len = putContent(headers, payload);
    return _socket.send(_chunkBuf, 0, len, flag);
  }

  public boolean routeInprocRef(ZmqFrames identities, int i, int flag) {
    assertSocket();
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
    assertSocket();
    _socket.subscribe(topic);
  }

  /**
   * Unsubscribe from topic.
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

  private int putContent(byte[] headers, byte[] payload) {
    putInt(_chunkBuf, 0, headers.length);
    System.arraycopy(headers, 0, _chunkBuf, 4, headers.length);

    int offset = 4 + headers.length;
    putInt(_chunkBuf, offset, payload.length);
    System.arraycopy(payload, 0, _chunkBuf, offset + 4, payload.length);

    return 4 + headers.length + 4 + payload.length;
  }

  private void putInt(byte[] buf, int offset, int i) {
    buf[offset] = (byte) (i >> 24);
    buf[++offset] = (byte) (i >> 16);
    buf[++offset] = (byte) (i >> 8);
    buf[++offset] = (byte) i;
  }
}
