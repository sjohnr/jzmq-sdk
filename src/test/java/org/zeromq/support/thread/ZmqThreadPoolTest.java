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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.support.exception.ExceptionHandler;

public class ZmqThreadPoolTest {

  static final Logger LOG = LoggerFactory.getLogger(ZmqThreadPoolTest.class);

  static class InterruptingExceptionHandler implements ExceptionHandler {
    @Override
    public void handleException(Throwable t) {
      Thread.currentThread().interrupt();
    }
  }

  static class ZmqRunnableContextPrototype implements ZmqRunnableContext {
    @Override
    public void execute() {
      // no-op.
    }

    @Override
    public void destroy() {
      // no-op.
    }

    @Override
    public void init() {
      // no-op.
    }
  }

  @Test
  public void t0() throws InterruptedException {
    LOG.info("Test ThreadPool: given InterruptableRunnable.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.init();

    Thread.sleep(300);

    LOG.info("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t1() throws InterruptedException {
    LOG.info("Test ThreadPool: few InterruptableRunnables.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.withRunnable(newInterruptableRunnable());
    target.init();

    Thread.sleep(300);

    LOG.info("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t2() throws InterruptedException {
    LOG.info(
        "Test ThreadPool: given InterruptableRunnable, FailingRunnable, " +
        "FailingRunnableAtInit, FailingRunnableAtDestroy.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.withRunnable(newFailingRunnable());
    target.withRunnable(newFailingRunnableAtInit());
    target.withRunnable(newFailingRunnableAtDestroy());
    target.init();

    Thread.sleep(300);

    LOG.info("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t3() {
    LOG.info("Test ThreadPool: check that blocked on thread_pool.waitOnMe() can be unblocked.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newFailingRunnableAtInit());
    target.init();

    target.blockOnMe();
  }

  private ZmqRunnable newInterruptableRunnable() {
    return ZmqRunnable.builder()
                      .withExceptionHandler(new InterruptingExceptionHandler())
                      .withRunnableContext(new ZmqRunnableContextPrototype() {
                        @Override
                        public void execute() {
                          try {
                            Thread.sleep(Long.MAX_VALUE);
                          }
                          catch (InterruptedException e) {
                            Thread.interrupted();
                            throw new RuntimeException(e);
                          }
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnable() {
    return ZmqRunnable.builder()
                      .withExceptionHandler(new InterruptingExceptionHandler())
                      .withRunnableContext(new ZmqRunnableContextPrototype() {
                        @Override
                        public void execute() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnableAtInit() {
    return ZmqRunnable.builder()
                      .withExceptionHandler(new InterruptingExceptionHandler())
                      .withRunnableContext(new ZmqRunnableContextPrototype() {
                        @Override
                        public void init() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnableAtDestroy() {
    return ZmqRunnable.builder()
                      .withExceptionHandler(new InterruptingExceptionHandler())
                      .withRunnableContext(new ZmqRunnableContextPrototype() {
                        @Override
                        public void destroy() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }
}
