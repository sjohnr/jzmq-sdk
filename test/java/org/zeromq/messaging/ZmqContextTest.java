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
import org.zeromq.ZMQ;

public class ZmqContextTest {

  @Test
  public void t0() {
    ZmqContext target = new ZmqContext();
    try {
      target.setThreadNum(2);
      target.init();
    }
    finally {
      target.destroy();
    }
  }

  @Test
  public void t1() {
    ZmqContext target = new ZmqContext();
    try {
      target.init();

      assert target.newSocket(ZMQ.ROUTER).getType() == ZMQ.ROUTER;
      assert target.newSocket(ZMQ.DEALER).getType() == ZMQ.DEALER;
      assert target.newSocket(ZMQ.PUB).getType() == ZMQ.PUB;
      assert target.newSocket(ZMQ.SUB).getType() == ZMQ.SUB;
      assert target.newSocket(ZMQ.PUSH).getType() == ZMQ.PUSH;
      assert target.newSocket(ZMQ.PULL).getType() == ZMQ.PULL;

      assert target.newPoller(1) != null;
    }
    finally {
      target.destroy();
    }
  }
}
