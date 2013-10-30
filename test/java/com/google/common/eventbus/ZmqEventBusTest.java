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

package com.google.common.eventbus;

import org.junit.Test;
import org.zeromq.messaging.ZmqException;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

public class ZmqEventBusTest {

  public static final String MESSAGE = "Thank you. Good bye.";

  static class Listener {

    @Subscribe
    public void event(DeadEvent event) {
      try {
        huevent();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void huevent() {
      throw new RuntimeException(MESSAGE);
    }
  }

  @Test
  public void t0() {
    ZmqEventBus target = new ZmqEventBus();
    target.register(new Listener());
    try {
      target.post(new Object());
      fail();
    }
    catch (ZmqException e) {
      assertEquals(ZmqException.ErrorCode.SEE_CAUSE, e.errorCode());
      assert e.getCause() != null && e.getCause() instanceof RuntimeException;
    }
  }
}
