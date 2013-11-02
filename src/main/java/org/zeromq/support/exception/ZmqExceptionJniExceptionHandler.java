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
public final class ZmqExceptionJniExceptionHandler extends AbstractExceptionHandlerInTheChain {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqExceptionJniExceptionHandler.class);

  @Override
  public void handleException(Throwable t) {
    if (org.zeromq.ZMQException.class.isAssignableFrom(t.getClass())) {
      org.zeromq.ZMQException e = (org.zeromq.ZMQException) t;
      ZMQ.Error error = null;
      int errorCode = e.getErrorCode();
      try {
        error = ZMQ.Error.findByCode(errorCode);
      }
      catch (Throwable ignore) {
      }
      if (error != null) {
        LOG.error("!!! Got zmq_exception: error_code={}, error={}.", errorCode, error);
      }
      else {
        LOG.error("!!! Got zmq_exception: error_code={}, error is unknown.", errorCode);
      }
      throw e;
    }
    next().handleException(t);
  }
}
