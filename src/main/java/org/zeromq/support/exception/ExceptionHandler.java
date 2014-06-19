package org.zeromq.support.exception;

/** Generic component for handling exceptional cases. */
public interface ExceptionHandler {

  void handleException(Throwable t);
}
