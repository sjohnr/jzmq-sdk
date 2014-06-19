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
