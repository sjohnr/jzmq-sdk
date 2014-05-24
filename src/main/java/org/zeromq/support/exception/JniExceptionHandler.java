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

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.messaging.ZmqException;

/**
 * Exception handler for hanling {@link org.zeromq.ZMQException}.
 * <p/>
 * Parses {@link org.zeromq.ZMQException#errorCode}, logs actual exception and re-throw it.
 */
public final class JniExceptionHandler extends AbstractExceptionHandlerInTheChain {

  private static final Logger LOG = LoggerFactory.getLogger(JniExceptionHandler.class);

  @Override
  public void handleException(Throwable t) {
    Throwable rc = Throwables.getRootCause(t);
    if (org.zeromq.ZMQException.class.isAssignableFrom(rc.getClass())) {
      org.zeromq.ZMQException e = (org.zeromq.ZMQException) rc;
      ZMQ.Error error = null;
      int errorCode = e.getErrorCode();
      try {
        error = ZMQ.Error.findByCode(errorCode);
      }
      catch (Throwable ignore) {
      }
      if (error != null) {
        LOG.error("!!! Got native_zmq_exception: error_code={}, error={}.", errorCode, error);
        throw ZmqException.wrappedNative(error); // catch native error and re-throw wrapped exception.
      }
      else {
        LOG.error("!!! Got native_zmq_exception: error_code={}, error is unknown.", errorCode);
        throw e; // re-throw unknown native exception.
      }
    }
    next().handleException(t);
  }
}
