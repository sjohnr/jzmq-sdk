package org.zeromq.support.exception;

import org.zeromq.messaging.ZmqException;

import static org.zeromq.messaging.ZmqException.ErrorCode.CONTEXT_NOT_ACCESSIBLE;
import static org.zeromq.messaging.ZmqException.ErrorCode.FATAL;
import static org.zeromq.messaging.ZmqException.ErrorCode.SEE_CAUSE;

/**
 * "Root" exception handler. Suggested to be a first exception handler in the chain.
 * Main function -- catch most fatal exceptions, like {@link ZmqException.ErrorCode#FATAL} and
 * {@link ZmqException.ErrorCode#CONTEXT_NOT_ACCESSIBLE}.
 */
public final class RootExceptionHandler extends AbstractExceptionHandlerInTheChain {

  @Override
  public void handleException(Throwable t) {
    if (ZmqException.class.isAssignableFrom(t.getClass())) {
      ZmqException e = (ZmqException) t;
      ZmqException.ErrorCode errorCode = e.code();
      if (errorCode == SEE_CAUSE) {
        handleException(e.getCause());
        return;
      }
      else if (errorCode == FATAL || errorCode == CONTEXT_NOT_ACCESSIBLE) {
        throw e;
      }
    }
    next().handleException(t);
  }
}
