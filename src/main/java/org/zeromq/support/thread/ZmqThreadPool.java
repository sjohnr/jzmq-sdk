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
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadPool wrapper abstraction. Works only with {@link ZmqRunnable}.
 * Overall this class is aimed to provide graceful shutdown for
 * submitted tasks.
 */
public final class ZmqThreadPool implements HasInit, HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqThreadPool.class);

  private static AtomicInteger _threadNum = new AtomicInteger();

  private static final ThreadFactory DAEMON_THREAD_FACTORY = new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("zmq-daemon-" + _threadNum.incrementAndGet());
      return t;
    }
  };

  private static final ThreadFactory NON_DAEMON_THREAD_FACTORY = new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(false);
      t.setName("zmq-" + _threadNum.incrementAndGet());
      return t;
    }
  };

  private List<ZmqRunnable> runnables = new ArrayList<ZmqRunnable>();

  private CountDownLatch _destroyLatch;
  private ThreadPoolExecutor _executor;

  //// CONSTRUCTORS

  private ZmqThreadPool() {
  }

  //// METHODS

  public static ZmqThreadPool newCachedDaemonThreadPool() {
    ZmqThreadPool target = new ZmqThreadPool();
    target._executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                                              Integer.MAX_VALUE,
                                              60L,
                                              TimeUnit.SECONDS,
                                              new SynchronousQueue<Runnable>(),
                                              DAEMON_THREAD_FACTORY);
    int i = target._executor.prestartAllCoreThreads();
    LOG.debug("Started {} core zmq-threads.", i);
    return target;
  }

  public static ZmqThreadPool newCachedNonDaemonThreadPool() {
    ZmqThreadPool target = new ZmqThreadPool();
    target._executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(),
                                              Integer.MAX_VALUE,
                                              60L,
                                              TimeUnit.SECONDS,
                                              new SynchronousQueue<Runnable>(),
                                              NON_DAEMON_THREAD_FACTORY);
    int i = target._executor.prestartAllCoreThreads();
    LOG.debug("Started {} core zmq-threads.", i);
    return target;
  }

  public ZmqThreadPool withRunnable(ZmqRunnable runnable) {
    this.runnables.add(runnable);
    return this;
  }

  public void blockOnMe() {
    try {
      _destroyLatch.await();
    }
    catch (InterruptedException ignore) {
    }
  }

  @Override
  public void init() {
    if (runnables.isEmpty()) {
      LOG.warn("ZmqThreadPool is empty!");
      return;
    }
    _destroyLatch = new CountDownLatch(runnables.size());
    for (ZmqRunnable runnable : runnables) {
      runnable.setDestroyLatch(_destroyLatch);
      _executor.submit(runnable);
    }
  }

  @Override
  public void destroy() {
    _executor.shutdownNow();
    if (_destroyLatch != null) {
      try {
        _destroyLatch.await();
      }
      catch (InterruptedException ignore) {
      }
    }
  }
}
