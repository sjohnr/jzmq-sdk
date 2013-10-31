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
import org.zeromq.TestRecorder;
import org.zeromq.messaging.ZmqException;

public class ZmqThreadPoolTest {

  private static class ZmqRunnableContextTemplate implements ZmqRunnableContext {

    @Override
    public void block() {
      // no-op.
    }

    @Override
    public void exec() throws ZmqException {
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
    TestRecorder r = new TestRecorder().start();
    r.log("Test ThreadPool: given InterruptableRunnable.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.init();

    Thread.sleep(300);

    r.log("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t1() throws InterruptedException {
    TestRecorder r = new TestRecorder().start();
    r.reset().start();
    r.log("Test ThreadPool: few InterruptableRunnables.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.withRunnable(newInterruptableRunnable());
    target.init();

    Thread.sleep(300);

    r.log("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t2() throws InterruptedException {
    TestRecorder r = new TestRecorder().start();
    r.log(
        "Test ThreadPool: given InterruptableRunnable, FailingRunnable, " +
        "FailingRunnableAtInit, FailingRunnableAtDestroy.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newInterruptableRunnable());
    target.withRunnable(newFailingRunnable());
    target.withRunnable(newFailingRunnableAtInit());
    target.withRunnable(newFailingRunnableAtDestroy());
    target.init();

    Thread.sleep(300);

    r.log("Destroying ThreadPool and asserting that there will be no hangout at .destroy().");

    target.destroy();
  }

  @Test
  public void t3() {
    TestRecorder r = new TestRecorder().start();
    r.log("Test ThreadPool: check that blocked on thread_pool.waitOnMe() maybe unblocked.");

    ZmqThreadPool target = ZmqThreadPool.newCachedDaemonThreadPool();
    target.withRunnable(newFailingRunnableAtInit());
    target.init();

    target.blockOnMe();
  }

  private ZmqRunnable newInterruptableRunnable() {
    return ZmqRunnable.builder()
                      .withRunnableContext(new ZmqRunnableContextTemplate() {
                        @Override
                        public void block() {
                          try {
                            Thread.sleep(Long.MAX_VALUE);
                          }
                          catch (InterruptedException e) {
                            Thread.interrupted();
                            throw new RuntimeException(e);
                          }
                        }

                        @Override
                        public void destroy() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnable() {
    return ZmqRunnable.builder()
                      .withRunnableContext(new ZmqRunnableContextTemplate() {
                        @Override
                        public void exec() throws ZmqException {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnableAtInit() {
    return ZmqRunnable.builder()
                      .withRunnableContext(new ZmqRunnableContextTemplate() {
                        @Override
                        public void init() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }

  private ZmqRunnable newFailingRunnableAtDestroy() {
    return ZmqRunnable.builder()
                      .withRunnableContext(new ZmqRunnableContextTemplate() {
                        @Override
                        public void destroy() {
                          throw new UnsupportedOperationException();
                        }
                      })
                      .build();
  }
}
