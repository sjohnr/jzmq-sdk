package org.zeromq.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulation over {@link ZMQ.Context} and lifecycle of {@link ZMQ.Socket}.
 * Hides from public clients {@link ZMQ.Context} and guarantees that one can create it and
 * safely can terminate it in non-blocking fashion. All zmq_sockets (when created) are
 * tracked so whenever client decides to terminate {@link ZmqContext} object --
 * all tracked zmq_sockets will also be closed.
 */
public final class ZmqContext implements HasInit, HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqContext.class);

  private int threadNum = 1;

  private ZMQ.Context _context;
  private List<ZMQ.Socket> _sockets = new ArrayList<ZMQ.Socket>();
  /**
   * Switcher which guards invariant. In particular
   * this flag prohibits creating new zmq_sockets when another
   * thread calling {@link #destroy()}.
   */
  private volatile boolean _accessible = true;

  //// METHODS

  public void setThreadNum(int threadNum) {
    if (threadNum > 0) {
      this.threadNum = threadNum;
    }
  }

  /** Method does some sanity check and then creates the {@link #_context}. */
  @Override
  public void init() {
    LOG.info("Creating ZmqContext(threadNum={}) ...", threadNum);
    _context = ZMQ.context(threadNum);
    LOG.info("ZmqContext created.");
  }

  /**
   * Thid method shall terminate {@link #_context}.
   * <p/>
   * <b>ALL</b> sockets which were produced by this ZmqContext
   * (and smartly gathered in the {@link #_sockets})
   * will be closed -- there by affecting <b>ALL</b> clients which
   * may be using these sockets.
   */
  @Override
  public synchronized void destroy() {
    try {
      if (!_accessible) {
        LOG.warn("Don't destroy ZmqContext twice.");
        return;
      }
      long regSockSize = _sockets.size();
      if (regSockSize > 0) {
        LOG.info("Destroying ZmqContext(closing {} sockets) ...", regSockSize);
      }
      else {
        LOG.info("Destroying ZmqContext ...");
      }
      for (ZMQ.Socket socket : _sockets) {
        try {
          socket.close();
          LOG.info("Closed socket.", socket);
        }
        catch (Exception e) {
          LOG.warn("Gobble exception at socket.close(): " + e, e);
        }
      }
      if (_context != null) {
        try {
          _context.term();
          LOG.info("Terminated ZmqContext.");
        }
        catch (Exception e) {
          LOG.warn("Gobble exception at ctx.term(): " + e, e);
        }
      }
    }
    finally {
      _accessible = false;
    }
  }

  /**
   * Creates instance of {@link ZMQ.Socket} and saves reference
   * in the {@link #_sockets}. Given {@code socketType} will be validated.
   *
   * @param socketType {@link ZMQ.Socket} type. Acceptable values are:
   *                   {@link ZMQ#DEALER}, {@link ZMQ#ROUTER}, {@link ZMQ#PUB}, {@link ZMQ#SUB},
   *                   {@link ZMQ#PUSH}, {@link ZMQ#PULL}.
   * @return newly created instance of {@link ZMQ.Socket}.
   */
  public synchronized ZMQ.Socket newSocket(int socketType) {
    if (!_accessible) {
      throw ZmqException.contextNotAccessible();
    }
    switch (socketType) {
      case ZMQ.REQ:
      case ZMQ.REP:
      case ZMQ.DEALER:
      case ZMQ.ROUTER:
      case ZMQ.PUB:
      case ZMQ.SUB:
      case ZMQ.XPUB:
      case ZMQ.XSUB:
      case ZMQ.PUSH:
      case ZMQ.PULL:
      case ZMQ.PAIR:
        break;
      default:
        throw ZmqException.fatal();
    }
    ZMQ.Socket socket = _context.socket(socketType);
    _sockets.add(socket);
    return socket;
  }

  public synchronized void closeSocket(ZMQ.Socket socket) {
    if (socket == null) {
      return;
    }
    Iterator<ZMQ.Socket> iter = _sockets.iterator();
    while (iter.hasNext()) {
      ZMQ.Socket _socket = iter.next();
      if (_socket == socket) {
        try {
          socket.close();
        }
        catch (Throwable e) {
          LOG.warn("Gobble exception at socket.close(): " + e, e);
        }
        iter.remove();
        break;
      }
    }
  }
}
