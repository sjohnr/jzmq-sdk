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

package org.zeromq.support.exception;

import org.junit.Test;
import org.zeromq.ZMQException;
import org.zeromq.messaging.ZmqException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class ZmqExceptionJniExceptionHandlerTest {

  @Test
  public void t0() {
    ZmqExceptionJniExceptionHandler target = new ZmqExceptionJniExceptionHandler();

    RuntimeException re = new RuntimeException();
    try {
      target.handleException(re);
      fail();
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.SEE_CAUSE, ((ZmqException) e).errorCode());
      assertSame(re, e.getCause());
    }

    Error err = new AssertionError();
    try {
      target.handleException(err);
      fail();
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.SEE_CAUSE, ((ZmqException) e).errorCode());
      assertSame(err, e.getCause());
    }
  }

  @Test
  public void t1() {
    ZmqExceptionJniExceptionHandler target = new ZmqExceptionJniExceptionHandler();

    ZMQException ze = new ZMQException("xyz", "xyz".hashCode());
    try {
      target.handleException(ze);
    }
    catch (Exception e) {
      assert e instanceof ZMQException;
      assertSame(ze, e);
    }
  }
}
