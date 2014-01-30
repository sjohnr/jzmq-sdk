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

import static org.junit.Assert.fail;
import static org.zeromq.messaging.ZmqException.ErrorCode.FATAL;

public class ZmqChannelTest extends ZmqAbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(ZmqChannelTest.class);

  @Test(expected = ZmqException.class)
  public void t0() {
    LOG.info("Test inproc:// connection behavior: connect first and bind second => exception.");

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withConnAddress(inprocAddr("service"))
                              .build())
              .build();

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withBindAddress(inprocAddr("service"))
                              .build())
              .build();
  }

  @Test
  public void t1() {
    LOG.info("Test inproc:// connection behavior: bind first and connect second => good.");

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withBindAddress(inprocAddr("service"))
                              .build())
              .build();

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withConnAddress(inprocAddr("service"))
                              .build())
              .build();
  }

  @Test(expected = ZmqException.class)
  public void t2() {
    LOG.info("Test inproc:// connection behavior: bind first and then connect several times.");

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withBindAddress(inprocAddr("service"))
                              .build())
              .build();

    ZmqChannel.DEALER(ctx())
              .withProps(Props.builder()
                              .withConnAddress(inprocAddr("service"))
                              .build())
              .withProps(Props.builder()
                              .withConnAddress(inprocAddr("service-noabc"))
                              .build())
              .build();
  }

  @Test
  public void t3() {
    LOG.info("Test edge settings.");

    ZmqChannel req = ZmqChannel.DEALER(ctx())
                               .withProps(Props.builder()
                                               .withHwmRecv(0)
                                               .withHwmSend(0)
                                               .withWaitSend(-1)
                                               .withWaitRecv(1)
                                               .withConnAddress(connAddr(4466))
                                               .build())
                               .build();

    ZMQ.Poller p = new ZMQ.Poller(1);
    req.watchSendRecv(p);

    p.poll(100);
    assert req.send(HELLO()); // well, you can send.
    assert req.send(HELLO()); // well, you can send.
    assert req.send(HELLO()); // well, you can send.

    assert !req.canRecv(); // you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t4() {
    LOG.info("Test edge settings.");

    ZmqChannel req = ZmqChannel.DEALER(ctx())
                               .withProps(Props.builder()
                                               .withHwmRecv(1)
                                               .withHwmSend(1)
                                               .withWaitSend(0)
                                               .withWaitRecv(0)
                                               .withConnAddress(connAddr(4466))
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
    assert !req.canSend();
    assert !req.send(HELLO()); // here, you will not block and will not send :|

    assert !req.canRecv(); // obviously you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t5() {
    LOG.info("Test wrong attempts to use channel.");

    ZmqChannel rep = ZmqChannel.ROUTER(ctx())
                               .withProps(Props.builder()
                                               .withBindAddress(bindAddr(6633))
                                               .build())
                               .build();

    // try reg channel twice.
    {
      ZMQ.Poller poller = new ZMQ.Poller(1);
      rep.watchSendRecv(poller); // reg once.
      try {
        rep.watchSendRecv(poller); // reg twice.
        fail();
      }
      catch (ZmqException e) {
        assert e.errorCode() == FATAL;
      }
    }

    // try again with more sophisticated case: use channel for a while and then reg it.
    {
      rep.destroy();
      rep = ZmqChannel.ROUTER(ctx())
                      .withProps(Props.builder()
                                      .withBindAddress(bindAddr(6633))
                                      .build())
                      .build();

      // use channel functions ...
      rep.watchSendRecv(new ZMQ.Poller(1));
      assert !rep.canRecv();
      assert !rep.canSend();
      // reg again and check that this fails.
      try {
        rep.watchSendRecv(new ZMQ.Poller(1));
        fail();
      }
      catch (ZmqException e) {
        assert e.errorCode() == FATAL;
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
        assert e.errorCode() == FATAL;
      }
    }
  }

  @Test
  public void t6() {
    LOG.info("Test poller operations on connected channel.");

    ZmqChannel client = ZmqChannel.DEALER(ctx())
                                  .withProps(Props.builder()
                                                  .withWaitSend(0)
                                                  .withWaitRecv(100)
                                                  .withConnAddress(connAddr(6677))
                                                  .build())
                                  .build();

    ZmqChannel server = ZmqChannel.ROUTER(ctx())
                                  .withProps(Props.builder()
                                                  .withWaitSend(0)
                                                  .withWaitRecv(100)
                                                  .withBindAddress(bindAddr(6677))
                                                  .build())
                                  .build();

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

    serverPoller.poll(1000);
    assert server.canRecv(); // at this point server has input.
    assert server.recv() != null; // recv once.
    serverPoller.poll(timeout);
    assert server.canRecv(); // still server has input.
    ZmqMessage req = server.recv(); // recv twice.
    assert req != null;
    serverPoller.poll(timeout);
    assert !server.canRecv(); // no more input for server.
    assert server.recv() == null; // and ofcourse you can't get input for server.
    ZmqMessage rep = ZmqMessage.builder(req)
                               .withPayload(WORLD().payload())
                               .build();
    assert server.send(rep); // reply to client.

    clientPoller.poll(timeout);
    assert client.canRecv(); // yes, client has input.
    assert client.recv() != null;
  }
}
