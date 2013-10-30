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

package org.zeromq.support.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.exception.ExceptionHandler;
import org.zeromq.support.exception.ExceptionHandlerTemplate;
import org.zeromq.support.exception.InterruptedExceptionHandler;
import org.zeromq.support.exception.UncaughtExceptionHandler;
import org.zeromq.support.exception.ZmqExceptionNativeLibExceptionHandler;

import java.util.concurrent.CountDownLatch;

public final class ZmqRunnable implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqRunnable.class);

  public static class Builder implements ObjectBuilder<ZmqRunnable> {

    private final ZmqRunnable _target = new ZmqRunnable();

    private Builder() {
    }

    public Builder withRunnableContext(ZmqRunnableContext runnableContext) {
      _target.runnableContext = runnableContext;
      return this;
    }

    @Override
    public void checkInvariant() {
      assert _target.runnableContext != null;
    }

    @Override
    public ZmqRunnable build() {
      checkInvariant();
      return _target;
    }
  }

  private static class InternalExceptionHandler extends ExceptionHandlerTemplate {
    @Override
    public void handleException(Throwable e) {
      if (ZmqException.class.isAssignableFrom(e.getClass())) {
        ZmqException target = (ZmqException) e;
        if (target.errorCode() == ZmqException.ErrorCode.SEE_CAUSE) {
          handleException(target.getCause());
          return;
        }
        else {
          LOG.error("!!! Got fatal issue: " + target, target);
          throw target;
        }
      }
      next().handleException(e);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InternalExceptionHandler withNext(ExceptionHandler nextExceptionHandler) {
      return super.withNext(nextExceptionHandler);
    }
  }

  /**
   * <b>Private</b> concurrent facility helping to stop threads more deterministically than usual
   * {@link java.util.concurrent.ExecutorService#shutdownNow()}.
   */
  private CountDownLatch destroyLatch;
  private ZmqRunnableContext runnableContext;

  private ExceptionHandler _exceptionHandlerChain =
      new InternalExceptionHandler().withNext(
          new ZmqExceptionNativeLibExceptionHandler().withNext(
              new InterruptedExceptionHandler().withNext(
                  new UncaughtExceptionHandler()
              )
          )
      );

  //// CONSTRUCTORS

  private ZmqRunnable() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  /**
   * <ul>
   * <li>method loops indefinitely calling <i>some client specified logic</i> util something is happened.</li>
   * <li>thread was interrupted => this is the loop exit.</li>
   * <li>got exception and {@link #_exceptionHandlerChain} failed to process it and re-thrown it => loop exit.</li>
   * </ul>
   */
  @Override
  public final void run() {
    try {
      runnableContext.init();
      // here we go ...
      while (!Thread.currentThread().isInterrupted()) {
        try {
          runnableContext.block();
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          runnableContext.exec();
        }
        catch (Exception e) {
          // catch exception using smart mechanism of chained exception handlers.
          // there're two exits at this point:
          // -- something really bad happened and one of the handlers in the chain raised exception.
          // -- something exceptional happened but not catastrophic and we can loop again.
          _exceptionHandlerChain.handleException(e);
        }
      }
    }
    catch (Throwable e) {
      LOG.error("!!! Unrecoverable issue just happened: " + e + ". Thank you. Good bye.", e);
    }
    finally {
      // this block is aimed to support following situations:
      // -- in case something unrecoverable happened in the "loop-until-interrupted".
      // -- in case thead had really been interrupted.
      try {
        runnableContext.destroy();
      }
      catch (Throwable e) {
        LOG.error("Gobble exception at runnable_ctx.destroy(): " + e, e);
      }
      finally {
        if (destroyLatch != null) {
          destroyLatch.countDown();
        }
      }
    }
  }

  /** <b>NOTE: this is SPI method called by {@link ZmqThreadPool}. Don't touch it.</b> */
  void setDestroyLatch(CountDownLatch destroyLatch) {
    this.destroyLatch = destroyLatch;
  }
}
