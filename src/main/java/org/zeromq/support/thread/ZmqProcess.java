package org.zeromq.support.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.support.HasInvariant;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.exception.ExceptionHandler;
import org.zeromq.support.exception.InterruptedExceptionHandler;
import org.zeromq.support.exception.JniExceptionHandler;
import org.zeromq.support.exception.LoggingExceptionHandler;
import org.zeromq.support.exception.RootExceptionHandler;

import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkArgument;

public final class ZmqProcess implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqProcess.class);

  private static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER_CHAIN =
      new RootExceptionHandler()
          .withNext(new JniExceptionHandler()
                        .withNext(new InterruptedExceptionHandler()
                                      .withNext(new LoggingExceptionHandler())));

  public static class Builder implements ObjectBuilder<ZmqProcess>, HasInvariant {

    private final ZmqProcess _target = new ZmqProcess();

    private Builder() {
    }

    public Builder withActor(ZmqActor actor) {
      _target.actor = actor;
      return this;
    }

    public Builder withExceptionHandler(ExceptionHandler exceptionHandler) {
      _target.exceptionHandler = exceptionHandler;
      return this;
    }

    @Override
    public void checkInvariant() {
      checkArgument(_target.actor != null);
    }

    @Override
    public ZmqProcess build() {
      checkInvariant();
      return _target;
    }
  }

  /**
   * <b>Private</b> concurrent facility helping to stop threads more deterministically than usual
   * {@link java.util.concurrent.ExecutorService#shutdownNow()}.
   */
  private CountDownLatch destroyLatch;
  private ZmqActor actor;
  private ExceptionHandler exceptionHandler = DEFAULT_EXCEPTION_HANDLER_CHAIN;

  //// CONSTRUCTORS

  private ZmqProcess() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  /**
   * <ul>
   * <li>method loops indefinitely calling <i>some client specified logic</i> util something is happened.</li>
   * <li>thread was interrupted => this is the loop exit.</li>
   * <li>got exception and {@link #exceptionHandler} failed to process it and re-thrown it => loop exit.</li>
   * </ul>
   */
  @Override
  public final void run() {
    try {
      actor.init();
      // here we go ...
      while (!Thread.currentThread().isInterrupted()) {
        try {
          actor.exec();
        }
        catch (Exception e) {
          // catch exception using mechanism of chained exception handlers.
          // there're two exits at this point:
          // -- something really bad happened and one of the handlers in the chain raised exception.
          // -- something exceptional happened but not catastrophic and we can loop again.
          exceptionHandler.handleException(e);
        }
      }
    }
    catch (Throwable e) {
      LOG.error("Got: " + e + ". About to destroy actor. Thank you and good bye.", e);
    }
    finally {
      // this block is aimed to support following situations:
      // -- in case something unrecoverable happened in the "loop-until-interrupted".
      // -- in case thead had really been interrupted.
      try {
        actor.destroy();
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

  /** <b>NOTE: this is SPI method for unit tests. Don't touch it.</b> */
  void setDestroyLatch(CountDownLatch destroyLatch) {
    this.destroyLatch = destroyLatch;
  }
}
