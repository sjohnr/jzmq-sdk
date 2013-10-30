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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * Exception handler for {@link org.zeromq.ZMQException}-s.
 * <p/>
 * Parses {@link org.zeromq.ZMQException#errorCode}, logs actual exception and re-throw it.
 */
public final class ZmqExceptionNativeLibExceptionHandler extends ExceptionHandlerTemplate {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqExceptionNativeLibExceptionHandler.class);

  @Override
  public void handleException(Throwable e) {
    if (org.zeromq.ZMQException.class.isAssignableFrom(e.getClass())) {
      org.zeromq.ZMQException nativeLibException = (org.zeromq.ZMQException) e;
      ZMQ.Error error = null;
      int errorCode = nativeLibException.getErrorCode();
      try {
        error = ZMQ.Error.findByCode(errorCode);
      }
      catch (Throwable t) {
        LOG.error("!!! Got zmq_exception: error_code={} is unknown.", errorCode);
      }
      if (error != null) {
        LOG.error("!!! Got zmq_error: error_code={}.", error.getCode());
      }
      throw nativeLibException;
    }
    next().handleException(e);
  }

  @SuppressWarnings("unchecked")
  @Override
  public ZmqExceptionNativeLibExceptionHandler withNext(ExceptionHandler nextExceptionHandler) {
    return super.withNext(nextExceptionHandler);
  }
}
