package org.zeromq.support.exception;

import com.google.common.base.Throwables;
import org.zeromq.messaging.ZmqException;

/**
 * Wrapping exception handler for {@link InterruptedException}.
 * <p/>
 * Simply logs exception occurence, wraps given {@link InterruptedException}
 * object into {@link org.zeromq.messaging.ZmqException} and re-throw it.
 */
public final class InterruptedExceptionHandler extends AbstractExceptionHandlerInTheChain {

  @Override
  public void handleException(Throwable t) {
    Throwable rc = Throwables.getRootCause(t);
    if (InterruptedException.class.isAssignableFrom(rc.getClass())) {
      Thread.interrupted();
      throw ZmqException.fatal();
    }
    next().handleException(t);
  }
}
