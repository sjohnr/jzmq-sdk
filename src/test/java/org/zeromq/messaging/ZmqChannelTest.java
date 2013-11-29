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
    LOG.info("Test settings.");

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
    assert req.send(HELLO()); // ... but! you can call .send() and ... that's it :|

    assert !req.hasInput(); // obviously you don't have input.
    assert req.recv() == null; // ... and you will not get it :|
  }
}
