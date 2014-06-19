package org.zeromq.support.thread;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ZmqThreadPool implements HasInit, HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqThreadPool.class);

  private List<ZmqProcess> processes = new ArrayList<ZmqProcess>();

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
                                              new ThreadFactoryBuilder().setDaemon(true)
                                                                        .setNameFormat("zmq-daemon")
                                                                        .build());
    return target;
  }

  public ZmqThreadPool withProcess(ZmqProcess process) {
    this.processes.add(process);
    return this;
  }

  @Override
  public void init() {
    if (processes.isEmpty()) {
      LOG.warn("ZmqThreadPool is empty!");
      return;
    }
    _destroyLatch = new CountDownLatch(processes.size());
    for (ZmqProcess p : processes) {
      p.setDestroyLatch(_destroyLatch);
      _executor.submit(p);
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
