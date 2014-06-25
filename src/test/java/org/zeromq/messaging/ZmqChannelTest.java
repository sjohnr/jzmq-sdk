package org.zeromq.messaging;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.support.exception.JniExceptionHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.zeromq.ZMQ.DONTWAIT;
import static org.zeromq.messaging.ZmqException.ErrorCode.FATAL;
import static org.zeromq.support.ZmqUtils.matchHeaders;
import static org.zeromq.support.ZmqUtils.matchIdentities;
import static org.zeromq.support.ZmqUtils.matchInprocRef;
import static org.zeromq.support.ZmqUtils.matchPayload;

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
              .withProps(Props.builder().withConnectAddr(inprocAddr("service-not-available")).build())
              .build();
  }

  @Test
  public void t3() {
    LOG.info("Test wrong attempts to use channel: register channel on poller twice, " +
             "destroy channel and access it, " +
             "call poller functions w/o registering channel on poller.");

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
  public void t4() {
    LOG.info("Test poller operations on connected DEALER/ROUTER channels.");

    ZmqChannel client = ZmqChannel.DEALER(ctx()).withProps(Props.builder()
                                                                .withConnectAddr(connAddr(6677))
                                                                .build()).build();
    ZmqChannel server = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6677)).build()).build();

    ZMQ.Poller client_poller = new ZMQ.Poller(1);
    client.watchRecv(client_poller);

    ZMQ.Poller server_poller = new ZMQ.Poller(1);
    server.watchRecv(server_poller);

    int timeout = 10;

    client_poller.poll(timeout);
    assert !client.canRecv(); // no input initially.

    server_poller.poll(timeout);
    assert !server.canRecv(); // no input initially.

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0); // send once.
    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0); // send twice.
    client_poller.poll(timeout);
    assert !client.canRecv(); // you don't have input yet (server not replied at this point).

    server_poller.poll(-1);
    assert server.canRecv(); // at this point server has input.
    assert server.recv(0) != null; // recv once.
    assert server.canRecv(); // still server has input.
    ZmqFrames req = server.recv(0); // recv twice.
    assert req != null;
    server_poller.poll(timeout); // clear poller events and get new ones.
    assert !server.canRecv(); // no more input for server.
    assert server.recv(0) == null; // and ofcourse you can't get input for server :|

    // send reply to client.
    assert server.sendFrames(req, 0);

    client_poller.poll(timeout); // checkout client!
    assert client.canRecv(); // yes, client has input.
    ZmqFrames recv = client.recv(0);
    assert recv != null;
  }

  @Test
  public void t5() {
    LOG.info("Test send/recv on connected DEALER/ROUTER channels.");

    ZmqChannel client = ZmqChannel.DEALER(ctx()).withProps(Props.builder()
                                                                .withConnectAddr(connAddr(6677))
                                                                .build()).build();
    ZmqChannel server = ZmqChannel.ROUTER(ctx()).withProps(Props.builder().withBindAddr(bindAddr(6677)).build()).build();

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    assert server.recv(DONTWAIT) == null; // at this point non-blocking .recv() returns null.
    assert server.recv(0) != null; // by turn, blocking .recv() blocks a bit and returns message.
  }

  @Test
  public void t6() {
    LOG.info("Test send with not-connected DEALER.");

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(connAddr(6677))
                                                  .withHwmSend(HWM_ONE)
                                                  .build())
                                  .build();

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0); // you can send once.
    assert !client.route(emptyIdentities(), emptyHeaders(), payload(), DONTWAIT); // yout can't send twice ;|
  }

  @Test
  public void t7() {
    LOG.info("Test send with not-connected ROUTER.");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(bindAddr(6677))
                                                  .withRouterMandatory()
                                                  .withHwmSend(HWM_ONE)
                                                  .build())
                                  .build();

    try {
      server.route(emptyIdentities(), emptyHeaders(), payload(), DONTWAIT);
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
  public void t8() {
    LOG.info("Test register/unregister channel on poller(s).");

    ZmqChannel channel = ZmqChannel.ROUTER(ctx())
                                   .withProps(Props.builder().withBindAddr(bindAddr(6677)).build())
                                   .build();

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
  public void t9() {
    LOG.info("Test connected ROUTER/DEALER: what happens when its queue is full.");

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

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    ZmqFrames req = server.recv(0);
    assert req != null;

    assert server.sendFrames(req, 0);
    assert server.sendFrames(req, 0); // send second message (but in fact it doesn't).
    assert server.sendFrames(req, 0); // send third message (but in fact it doesn't).

    assert client.recv(0) != null;
    assert client.recv(0) == null; // second message has been silently dropped.
    assert client.recv(0) == null; // thrird message has been silently dropped.
  }

  @Test
  public void t10() {
    LOG.info("Test inprocRef: basic functioning.");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(inprocAddr("inprocRefTest"))
                                                  .withRouterMandatory()
                                                  .build())
                                  .build();

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(inprocAddr("inprocRefTest"))
                                                  .build())
                                  .build();

    client.sendInprocRef(0, DONTWAIT);
    client.sendInprocRef(42, DONTWAIT);
    client.sendInprocRef(Integer.MAX_VALUE, DONTWAIT);

    ZmqFrames ref0 = server.recv(0);
    assertNotNull(ref0);
    assertEquals(0, matchInprocRef(ref0));

    ZmqFrames ref42 = server.recv(0);
    assertNotNull(ref42);
    assertEquals(42, matchInprocRef(ref42));

    ZmqFrames refMax = server.recv(0);
    assertNotNull(refMax);
    assertEquals(Integer.MAX_VALUE, matchInprocRef(refMax));
  }

  @Test
  public void t11() {
    LOG.info("Test connected ROUTER/DEALER: what happens when its queue is 1.");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(bindAddr(6677))
                                                  .withHwmSend(HWM_UNLIMITED)
                                                  .withHwmRecv(HWM_UNLIMITED)
                                                  .build())
                                  .build();

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(connAddr(6677))
                                                  .withHwmSend(HWM_ONE)
                                                  .withHwmRecv(HWM_ONE)
                                                  .build())
                                  .build();

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    assert server.recv(0) != null;

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    assert server.recv(0) != null;

    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    assert server.recv(0) != null;
  }

  @Test
  public void t12() {
    LOG.info("Test matching content on connected DEALER/ROUTER: " +
             "[identities|headers|payload], " +
             "[identities|[]|payload], " +
             "[identities|headers|[]], " +
             "[identities|[]|[]].");

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withBindAddr(bindAddr(6677))
                                                  .withRouterMandatory()
                                                  .build())
                                  .build();

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withConnectAddr(connAddr(6677))
                                                  .withIdentity("client")
                                                  .build())
                                  .build();

    assert client.route(emptyIdentities(), headers(), payload(), 0);
    assert client.route(emptyIdentities(), emptyHeaders(), payload(), 0);
    assert client.route(emptyIdentities(), headers(), emptyPayload(), 0);
    assert client.route(emptyIdentities(), emptyHeaders(), emptyPayload(), 0);

    ZmqFrames recv0 = server.recv(0);
    assertEquals("client", new String(matchIdentities(recv0).get(0)));
    assertEquals("x=x,y=y,z=z", new String(matchHeaders(recv0)));
    assertEquals("payload", new String(matchPayload(recv0)));
    assert server.sendFrames(recv0, 0);

    ZmqFrames recv1 = server.recv(0);
    assertEquals("client", new String(matchIdentities(recv1).get(0)));
    assertEquals("", new String(matchHeaders(recv1)));
    assertEquals("payload", new String(matchPayload(recv1)));
    assert server.sendFrames(recv1, 0);

    ZmqFrames recv2 = server.recv(0);
    assertEquals("client", new String(matchIdentities(recv2).get(0)));
    assertEquals("x=x,y=y,z=z", new String(matchHeaders(recv2)));
    assertEquals("", new String(matchPayload(recv2)));
    assert server.sendFrames(recv2, 0);

    ZmqFrames recv3 = server.recv(0);
    assertEquals("client", new String(matchIdentities(recv3).get(0)));
    assertEquals("", new String(matchHeaders(recv3)));
    assertEquals("", new String(matchPayload(recv3)));
    assert server.sendFrames(recv3, 0);

    recv0 = client.recv(0);
    recv1 = client.recv(0);
    recv2 = client.recv(0);
    recv3 = client.recv(0);

    assertEquals(0, matchIdentities(recv0).size());
    assertEquals("x=x,y=y,z=z", new String(matchHeaders(recv0)));
    assertEquals("payload", new String(matchPayload(recv0)));

    assertEquals(0, matchIdentities(recv1).size());
    assertEquals("", new String(matchHeaders(recv1)));
    assertEquals("payload", new String(matchPayload(recv1)));

    assertEquals(0, matchIdentities(recv2).size());
    assertEquals("x=x,y=y,z=z", new String(matchHeaders(recv2)));
    assertEquals("", new String(matchPayload(recv2)));

    assertEquals(0, matchIdentities(recv3).size());
    assertEquals("", new String(matchHeaders(recv3)));
    assertEquals("", new String(matchPayload(recv3)));
  }
}
