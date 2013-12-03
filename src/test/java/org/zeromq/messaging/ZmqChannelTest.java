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

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withConnectAddress("inproc://service")
              .build();

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withBindAddress("inproc://service")
              .build();
  }

  @Test
  public void t1() {
    LOG.info("Test inproc:// connection behavior: bind first and connect second => good.");

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withBindAddress("inproc://service")
              .build();

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withConnectAddress("inproc://service")
              .build();
  }

  @Test(expected = ZmqException.class)
  public void t2() {
    LOG.info("Test inproc:// connection behavior: bind first and then connect several times.");

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withBindAddress("inproc://service")
              .build();

    ZmqChannel.builder()
              .withZmqContext(zmqContext())
              .ofDEALERType()
              .withConnectAddress("inproc://service")
              .withConnectAddress("inproc://service-nonabc")
              .build();
  }

  @Test
  public void t3() {
    LOG.info("Test edge settings.");

    ZmqChannel req = ZmqChannel.builder()
                               .withZmqContext(zmqContext())
                               .ofDEALERType()
                               .withHwmForRecv(0)
                               .withHwmForSend(0)
                               .withWaitOnSend(-1)
                               .withWaitOnRecv(1)
                               .withConnectAddress("tcp://localhost:4466")
                               .build();

    ZMQ.Poller p = zmqContext().newPoller(1);
    req.watchSendRecv(p);

    p.poll(100);
    assert req.send(HELLO()); // well, you can send, but that's not actual send :|
    assert req.send(HELLO()); // well, you can send but that's not actual send :|
    assert req.send(HELLO()); // well, you can send but that's not actual send :|

    assert !req.canRecv(); // obviously you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t4() {
    LOG.info("Test edge settings.");

    ZmqChannel req = ZmqChannel.builder()
                               .withZmqContext(zmqContext())
                               .ofDEALERType()
                               .withHwmForRecv(1)
                               .withHwmForSend(1)
                               .withWaitOnSend(0)
                               .withWaitOnRecv(0)
                               .withConnectAddress("tcp://localhost:4466")
                               .build();

    ZMQ.Poller p = zmqContext().newPoller(1);
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

    ZmqChannel rep = ZmqChannel.builder()
                               .withZmqContext(zmqContext())
                               .ofROUTERType()
                               .withBindAddress("tcp://*:6633")
                               .build();

    // try register channel twice.
    {
      ZMQ.Poller poller = zmqContext().newPoller(1);
      rep.watchSendRecv(poller); // register once.
      try {
        rep.watchSendRecv(poller); // register twice.
        fail();
      }
      catch (ZmqException e) {
        assert e.errorCode() == FATAL;
      }
    }

    // try again with more sophisticated case: use channel for a while and then register it.
    {
      rep.destroy();
      rep = ZmqChannel.builder()
                      .withZmqContext(zmqContext())
                      .ofROUTERType()
                      .withBindAddress("tcp://*:6633")
                      .build();

      // use channel functions ...
      rep.watchSendRecv(zmqContext().newPoller(1));
      assert !rep.canRecv();
      assert !rep.canSend();
      // register again and check that this fails.
      try {
        rep.watchSendRecv(zmqContext().newPoller(1));
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

    ZmqChannel client = ZmqChannel.builder()
                                  .withZmqContext(zmqContext())
                                  .ofDEALERType()
                                  .withWaitOnSend(0)
                                  .withWaitOnRecv(100)
                                  .withConnectAddress("tcp://localhost:6677")
                                  .build();

    ZmqChannel server = ZmqChannel.builder()
                                  .withZmqContext(zmqContext())
                                  .ofROUTERType()
                                  .withWaitOnSend(0)
                                  .withWaitOnRecv(100)
                                  .withBindAddress("tcp://*:6677")
                                  .build();

    ZMQ.Poller clientPoller = zmqContext().newPoller(1);
    client.watchRecv(clientPoller);

    ZMQ.Poller serverPoller = zmqContext().newPoller(1);
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
