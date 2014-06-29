package org.zeromq.support.exception;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqException;

/**
 * Wrapping exception handler for {@link InterruptedException}.
 * <p/>
 * Simply logs exception occurence, wraps given {@link InterruptedException}
 * object into {@link org.zeromq.messaging.ZmqException} and re-throw it.
 */
public final class InterruptedExceptionHandler extends AbstractExceptionHandlerInTheChain {

  private static final Logger LOG = LoggerFactory.getLogger(InterruptedExceptionHandler.class);

  @Override
  public void handleException(Throwable t) {
    Throwable rc = Throwables.getRootCause(t);
    if (InterruptedException.class.isAssignableFrom(rc.getClass())) {
      Thread.interrupted();
      LOG.warn("!!! Thread has been interrupted.");
      throw ZmqException.fatal();
    }
    next().handleException(t);
  }
}
