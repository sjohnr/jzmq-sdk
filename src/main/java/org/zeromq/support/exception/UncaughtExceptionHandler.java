package org.zeromq.support.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqException;

/**
 * Handle exception which has been unexpected et al or been
 * forgotten to be handled due to programming mistake.
 */
public final class UncaughtExceptionHandler implements ExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptionHandler.class);

  @Override
  public void handleException(Throwable t) {
    LOG.error("!!! Got uncaught exception: " + t, t);
    throw ZmqException.seeCause(t);
  }
}
