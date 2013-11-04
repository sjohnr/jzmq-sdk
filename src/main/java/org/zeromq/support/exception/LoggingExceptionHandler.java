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

/**
 * This exception handler simply logs an exception and
 * doesn't call {@code next_handler} in the chain.
 */
public final class LoggingExceptionHandler implements ExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(LoggingExceptionHandler.class);

  @Override
  public void handleException(Throwable t) {
    LOG.error("Got unhandled issue: " + t, t);
  }
}