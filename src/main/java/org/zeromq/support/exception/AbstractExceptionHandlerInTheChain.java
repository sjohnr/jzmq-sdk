package org.zeromq.support.exception;

import org.zeromq.support.InTheChain;

import static com.google.common.base.Preconditions.checkArgument;

public abstract class AbstractExceptionHandlerInTheChain implements ExceptionHandler, InTheChain<ExceptionHandler> {

  /**
   * Next exception handler in the chain.
   * <p/>
   * <b>NOTE: by default initialized to {@link UncaughtExceptionHandler}. This field isn't optional.</b>
   */
  private ExceptionHandler nextHandler = new UncaughtExceptionHandler();

  @Override
  public final AbstractExceptionHandlerInTheChain withNext(ExceptionHandler nextHandler) {
    checkArgument(nextHandler != null);
    this.nextHandler = nextHandler;
    return this;
  }

  @Override
  public final ExceptionHandler next() {
    return nextHandler;
  }
}
