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
    LOG.info("Test some interesting settings.");

    ZmqChannel req = ZmqChannel.builder()
                               .withZmqContext(zmqContext())
                               .ofDEALERType()
                               .withHwmForRecv(0)
                               .withHwmForSend(0)
                               .withWaitOnSend(1)
                               .withWaitOnRecv(250)
                               .withConnectAddress("tcp://localhost:4567")
                               .build();

    assert !req.hasOutput(); // some nice info: seems like you can't send a message.
    assert !req.send(HELLO()); // no, you can't :|

    assert !req.hasInput(); // obviously you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }

  @Test
  public void t4() {
    LOG.info("Test some wrong attempts to use channel.");

    ZmqChannel rep = ZmqChannel.builder()
                               .withZmqContext(zmqContext())
                               .ofROUTERType()
                               .withBindAddress("tcp://*:6633")
                               .build();

    // try register channel twoce.
    {
      ZMQ.Poller poller = zmqContext().newPoller(1);
      rep.register(poller); // register once.
      try {
        rep.register(poller); // register twice.
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
      assert !rep.hasInput();
      assert !rep.hasOutput();

      // register again and check that this fails.
      try {
        rep.register(zmqContext().newPoller(1));
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
        rep.hasInput();
        fail();
      }
      catch (ZmqException e) {
        assert e.errorCode() == FATAL;
      }
    }
  }

  @Test
  public void t5() throws InterruptedException {
    LOG.info("Test poller operations on connected channel.");

    ZmqChannel c = ZmqChannel.builder()
                             .withZmqContext(zmqContext())
                             .ofDEALERType()
                             .withConnectAddress("tcp://localhost:3388")
                             .build();

    ZmqChannel s = ZmqChannel.builder()
                             .withZmqContext(zmqContext())
                             .ofROUTERType()
                             .withBindAddress("tcp://*:3388")
                             .build();

    // TODO artemv: write some tests.
  }
}
