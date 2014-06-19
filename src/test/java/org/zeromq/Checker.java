package org.zeromq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public final class Checker implements Thread.UncaughtExceptionHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Checker.class);

  private final AtomicReference<Throwable> t = new AtomicReference<Throwable>();

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    this.t.set(e);
    LOG.error("e: " + e, e);
  }

  public boolean passed() {
    return t.get() == null;
  }
}
