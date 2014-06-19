package org.zeromq.messaging;

import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;
import org.zeromq.support.thread.ZmqProcess;
import org.zeromq.support.thread.ZmqThreadPool;

import java.util.ArrayList;
import java.util.List;

public class BaseFixture implements HasInit, HasDestroy {

  private final List<HasDestroy> _destroyables = new ArrayList<HasDestroy>();
  private final ZmqThreadPool _threadPool = ZmqThreadPool.newCachedDaemonThreadPool();

  @Override
  public final void init() {
    _threadPool.init();
  }

  @Override
  public final void destroy() {
    _threadPool.destroy();
    for (HasDestroy i : _destroyables) {
      i.destroy();
    }
  }

  public final void with(HasDestroy d) {
    assert d != null;
    _destroyables.add(d);
  }

  public final void with(ZmqProcess r) {
    assert r != null;
    _threadPool.withProcess(r);
  }
}
