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
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.messaging.ZmqException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class JniExceptionHandlerTest {

  @Test
  public void t0() {
    JniExceptionHandler target = new JniExceptionHandler();

    RuntimeException runtimeException = new RuntimeException();
    try {
      target.handleException(runtimeException);
      fail();
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.SEE_CAUSE, ((ZmqException) e).code());
      assertSame(runtimeException, e.getCause());
    }

    Error assertionError = new AssertionError();
    try {
      target.handleException(assertionError);
      fail();
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.SEE_CAUSE, ((ZmqException) e).code());
      assertSame(assertionError, e.getCause());
    }
  }

  @Test
  public void t1() {
    JniExceptionHandler target = new JniExceptionHandler();

    ZMQException nativeZMQException = new ZMQException("x-y-z", (int) ZMQ.Error.EHOSTUNREACH.getCode());
    try {
      target.handleException(nativeZMQException);
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.NATIVE_ERROR, ((ZmqException) e).code());
      assertEquals(ZMQ.Error.EHOSTUNREACH, ((ZmqException) e).nativeError());
    }
  }

  @Test
  public void t2() {
    JniExceptionHandler target = new JniExceptionHandler();

    ZmqException zmqException = ZmqException.seeCause(new ZMQException("a-b-c", (int) ZMQ.Error.EHOSTUNREACH.getCode()));
    try {
      target.handleException(zmqException);
    }
    catch (Exception e) {
      assert e instanceof ZmqException;
      assertEquals(ZmqException.ErrorCode.NATIVE_ERROR, ((ZmqException) e).code());
      assertEquals(ZMQ.Error.EHOSTUNREACH, ((ZmqException) e).nativeError());
    }
  }

  @Test
  public void t3() {
    JniExceptionHandler target = new JniExceptionHandler();

    ZmqException zmqException = ZmqException.seeCause(new ZMQException("unknown", "unknown".hashCode()));
    try {
      target.handleException(zmqException);
    }
    catch (Exception e) {
      assert e instanceof ZMQException;
    }
  }
}
