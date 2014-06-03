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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.support.exception.JniExceptionHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqException.ErrorCode.FATAL;
import static org.zeromq.messaging.ZmqException.ErrorCode.SEE_CAUSE;

public class ZmqChannelTest extends ZmqAbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(ZmqChannelTest.class);

  static final int HWM_UNLIMITED = 0;
  static final int HWM_ONE = 1;

  @Test(expected = ZmqException.class)
  public void t0() {
    LOG.info("Test inproc:// connection behavior: connect first and bind second => exception.");

    ZmqChannel.DEALER(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("service")).build()).build();
    ZmqChannel.DEALER(ctx()).withProps(Props.builder().withBindAddr(inprocAddr("service")).build()).build();
  }

  @Test
  public void t1() {
    LOG.info("Test inproc:// connection behavior: bind first and connect second => good.");

    ZmqChannel.DEALER(ctx()).withProps(Props.builder().withBindAddr(inprocAddr("service")).build()).build();
    ZmqChannel.DEALER(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("service")).build()).build();
  }

  @Test(expected = ZmqException.class)
  public void t2() {
    LOG.info("Test inproc:// connection behavior: bind first and then connect several times.");

    ZmqChannel.DEALER(ctx()).withProps(Props.builder().withBindAddr(inprocAddr("service")).build()).build();

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder().withConnectAddr(inprocAddr("service")).build())
              .withProps(Props.builder()
                             .withConnectAddr(inprocAddr("service-noabc")) /* not available inproc address */
                             .build())
              .build();
  }

  @Test
  public void t3() {
    LOG.info("Test edge settings: HWM_UNLIMITED.");

    ZmqChannel req = ZmqChannel.DEALER(ctx())
                               .withProps(Props.builder()
                                               .withHwmRecv(HWM_UNLIMITED)
                                               .withHwmSend(HWM_UNLIMITED)
                                               .withSendTimeout(-1)
                                               .withConnectAddr(connAddr(4466))
                                               .build())
                               .build();

    ZMQ.Poller p = new ZMQ.Poller(1);
    req.watchSendRecv(p);

    assert req.send(HELLO()); // you can send.
    assert req.send(HELLO()); // you can send.
    assert req.send(HELLO()); // you can send.

    p.poll(100);
    assert !req.canRecv(); // you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t4() {
    LOG.info("Test edge settings: HWM_ONE.");

    ZmqChannel req = ZmqChannel.DEALER(ctx())
                               .withProps(Props.builder()
                                               .withHwmRecv(HWM_ONE)
                                               .withHwmSend(HWM_ONE)
                                               .withSendTimeout(0)
                                               .withRecvTimeout(0)
                                               .withConnectAddr(connAddr(4466))
                                               .build())
                               .build();

    ZMQ.Poller p = new ZMQ.Poller(1);
    req.watchSendRecv(p);

    p.poll(100);
    assert req.canSend();
    assert req.send(HELLO()); // you can send w/o blocking once :|

    p.poll(100);
    assert !req.canSend();
    assert !req.send(HELLO()); // here, you will not block and will not send :|

    assert !req.canRecv(); // obviously you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t5() {
    LOG.info("Test wrong attempts to use channel: register channel on poller twice, " +
             "destroy channel and access it, " +
             "call poller' based functions w/o registering channel on poller.");

    ZmqChannel rep = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6633)).build()).build();

    // try reg channel twice.
    {
      ZMQ.Poller poller = new ZMQ.Poller(1);
      rep.watchSendRecv(poller); // reg once.
      try {
        rep.watchSendRecv(poller); // reg twice.
        fail();
      }
      catch (ZmqException e) {
        assert e.code() == FATAL;
      }
    }
    // destroy channel and after that try to access it.
    {
      rep.destroy();
      try {
        rep.canRecv();
        fail();
      }
      catch (ZmqException e) {
        assert e.code() == FATAL;
      }
    }
    // call .canRecv() without registering channel on poller.
    {
      rep = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6633)).build()).build();
      try {
        rep.canRecv();
        fail();
      }
      catch (ZmqException e) {
        assert e.code() == FATAL;
      }
    }
  }

  @Test
  public void t6() {
    LOG.info("Test poller operations on connected channel.");

    ZmqChannel client = ZmqChannel.DEALER(ctx()).withProps(Props.builder()
                                                                .withConnectAddr(connAddr(6677))
                                                                .build()).build();
    ZmqChannel server = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6677)).build()).build();

    ZMQ.Poller clientPoller = new ZMQ.Poller(1);
    client.watchRecv(clientPoller);

    ZMQ.Poller serverPoller = new ZMQ.Poller(1);
    server.watchRecv(serverPoller);

    int timeout = 10;

    clientPoller.poll(timeout);
    assert !client.canRecv(); // no input initially.

    serverPoller.poll(timeout);
    assert !server.canRecv(); // no input initially.

    assert client.send(HELLO()); // send once.
    assert client.send(HELLO()); // send twice.
    clientPoller.poll(timeout);
    assert !client.canRecv(); // you don't have input yet (server not replied at this point).

    serverPoller.poll(-1);
    assert server.canRecv(); // at this point server has input.
    assert server.recv() != null; // recv once.
    assert server.canRecv(); // still server has input.
    ZmqMessage req = server.recv(); // recv twice.
    assert req != null;
    serverPoller.poll(timeout); // clear poller events and get new ones.
    assert !server.canRecv(); // no more input for server.
    assert server.recv() == null; // and ofcourse you can't get input for server :|

    // send reply to client.
    ZmqMessage reply = ZmqMessage.builder(req).withPayload(WORLD().payload()).build();
    assert server.send(reply);

    clientPoller.poll(timeout); // checkout client!
    assert client.canRecv(); // yes, client has input.
    assert client.recv() != null;
  }

  @Test
  public void t7() {
    LOG.info("Test .recvDontWait()/.recv() operations.");

    ZmqChannel client = ZmqChannel.DEALER(ctx()).withProps(Props.builder().withConnectAddr(connAddr(6677)).build()).build();
    ZmqChannel server = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6677)).build()).build();

    assert client.send(HELLO());
    assert server.recvDontWait() == null; // at this point non-blocking .recv() returns null.
    assert server.recv() != null; // by turn, blocking .recv() blocks a bit and returns message.
  }

  @Test
  public void t8() {
    LOG.info("Test .send() with DONT_WAIT operations and HWM_ONE.");

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(connAddr(6677))
                                                  .withHwmSend(HWM_ONE)
                                                  .build())
                                  .build();

    assert client.send(HELLO()); // you can send once.
    assert !client.send(HELLO()); // yout can't send twice ;|
  }

  @Test
  public void t9() {
    LOG.info("Test router_mandatory setting: what happens when it comes that destination unreachable.");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(bindAddr(6677))
                                                  .withRouterMandatory()
                                                  .build())
                                  .build();

    try {
      server.send(HELLO());
      fail();
    }
    catch (Exception e) {
      try {
        new JniExceptionHandler().handleException(e);
      }
      catch (Exception e1) {
        assert e1 instanceof ZmqException;
        assertEquals(ZmqException.ErrorCode.NATIVE_ERROR, ((ZmqException) e1).code());
        assertEquals(ZMQ.Error.EHOSTUNREACH, ((ZmqException) e1).nativeError());
      }
    }
  }

  @Test
  public void t10() {
    LOG.info("Test that you can register/unregister channel on pollers several times.");

    ZmqChannel channel = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6677)).build()).build();

    ZMQ.Poller p = new ZMQ.Poller(1);
    channel.watchRecv(p);
    p.poll(100);
    assert !channel.canRecv();
    channel.unregister(); // unregistering poller.

    p = new ZMQ.Poller(1); // new poller.
    channel.watchRecv(p); // can call functions on new poller.
    p.poll(100);
    assert !channel.canRecv();
    channel.unregister();
  }

  @Test
  public void t11() {
    LOG.info("Test router: what happens when its queue is full.");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(bindAddr(6677))
                                                  .withRouterMandatory()
                                                  .withHwmSend(HWM_ONE)
                                                  .withHwmRecv(HWM_ONE)
                                                  .build())
                                  .build();

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(connAddr(6677))
                                                  .withHwmSend(HWM_UNLIMITED)
                                                  .withHwmRecv(HWM_UNLIMITED)
                                                  .build())
                                  .build();

    client.send(HELLO());
    ZmqMessage hello = server.recv();
    assert hello != null;

    server.send(ZmqMessage.builder(WORLD()).withIdentities(hello.identityFrames()).build());
    server.send(ZmqMessage.builder(WORLD()).withIdentities(hello.identityFrames()).build()); // send second message.
    server.send(ZmqMessage.builder(WORLD()).withIdentities(hello.identityFrames()).build()); // send third message.

    assert client.recv() != null;
    assert client.recv() == null; // second message has been silently dropped.
    assert client.recv() == null; // thrird message has been silently dropped.
  }
}
