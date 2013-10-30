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

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.zeromq.TestRecorder;
import org.zeromq.messaging.device.CallerHeaders;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.zeromq.messaging.ZmqAbstractTest.Fixture.HELLO;

public class ZmqChannelFactoryTest extends ZmqAbstractTest {

  @Test(expected = ZmqException.class)
  public void t0() {
    TestRecorder r = new TestRecorder().start();
    r.log("Test inproc:// connection behavior: connect first and bind second => exception.");

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withConnectAddress("inproc://service")
                     .build()
                     .newChannel();

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withBindAddress("inproc://service")
                     .build()
                     .newChannel();
  }

  @Test
  public void t1() {
    TestRecorder r = new TestRecorder().start();
    r.log("Test inproc:// connection behavior: bind first and connect second => good.");

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withBindAddress("inproc://service")
                     .build()
                     .newChannel();

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withConnectAddress("inproc://service")
                     .build()
                     .newChannel();
  }

  @Test(expected = ZmqException.class)
  public void t2() {
    TestRecorder r = new TestRecorder().start();
    r.log("Test inproc:// connection behavior: bind first and then connect several times.");

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withBindAddress("inproc://service")
                     .build()
                     .newChannel();

    ZmqChannelFactory.builder()
                     .withZmqContext(zmqContext())
                     .ofDEALERType()
                     .withConnectAddress("inproc://service")
                     .withConnectAddress("inproc://service-huervice")
                     .build()
                     .newChannel();
  }

  @Test
  public void t3_perf() {
    TestRecorder r = new TestRecorder().start();
    r.log("Perf test for simple message .send() via DEALER to not-available peer. Just .send() and measure.");

    int HWM = 1000000;
    ZmqChannel channel = ZmqChannelFactory.builder()
                                          .ofDEALERType()
                                          .withZmqContext(zmqContext())
                                          .withConnectAddress("tcp://localhost:8833")
                                          .withHwmForSend(HWM)
                                          .withHwmForSend(HWM)
                                          .build()
                                          .newChannel();
    ZmqMessage src = HELLO();

    ZmqFrames identities = new ZmqFrames();
    identities.add("x".getBytes());
    identities.add("y".getBytes());
    identities.add("z".getBytes());

    int ITER = 100;
    int MESSAGE_NUM = 1000;
    ZmqMessage message = ZmqMessage.builder(src)
                                   .withIdentities(identities)
                                   .withHeaders(new CallerHeaders().setCorrId(123456789l))
                                   .build();
    Stopwatch timer = new Stopwatch().start();
    for (int j = 0; j < ITER; j++) {
      for (int i = 0; i < MESSAGE_NUM; i++) {
        assert channel.send(message);
      }
    }
    r.logQoS((ITER * MESSAGE_NUM) / timer.stop().elapsedTime(MILLISECONDS), "messages/ms.");
  }
}
