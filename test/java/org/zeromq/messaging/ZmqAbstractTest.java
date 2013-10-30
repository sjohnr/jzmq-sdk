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

import org.junit.After;
import org.junit.Before;

public abstract class ZmqAbstractTest {

  public static final long MESSAGE_NUM = 100;

  public static class Fixture {

    public static ZmqMessage HELLO() {
      return ZmqMessage.builder().withPayload("hello".getBytes()).build();
    }

    public static ZmqMessage WORLD() {
      return ZmqMessage.builder().withPayload("world".getBytes()).build();
    }

    public static ZmqMessage CARP() {
      return ZmqMessage.builder().withPayload("carp".getBytes()).build();
    }

    public static ZmqMessage SHIRT() {
      return ZmqMessage.builder().withPayload("shirt".getBytes()).build();
    }
  }

  private final ZmqContext _ctx = new ZmqContext();

  protected final ZmqContext zmqContext() {
    return _ctx;
  }

  @Before
  public final void init() {
    _ctx.init();
  }

  @After
  public final void cleanup() {
    _ctx.destroy();
  }
}
