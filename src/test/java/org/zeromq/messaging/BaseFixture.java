package org.zeromq.messaging;

import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;
import org.zeromq.support.thread.ZmqProcess;
import org.zeromq.support.thread.ZmqThreadPool;

import java.util.ArrayList;
import java.util.List;

public class BaseFixture implements HasInit, HasDestroy {

  protected final ZmqContext ctx;
  private final List<HasDestroy> _destroyables = new ArrayList<HasDestroy>();
  private final ZmqThreadPool _threadPool = ZmqThreadPool.newCachedDaemonThreadPool();

  protected BaseFixture(ZmqContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public final void init() {
    _threadPool.init();
  }

  @Override
  public final void destroy() {
    _threadPool.destroy();
    for (HasDestroy hasDestroy : _destroyables) {
      hasDestroy.destroy();
    }
  }

  @SuppressWarnings("unchecked")
  public final <T> T with(HasDestroy hasDestroy) {
    assert hasDestroy != null;
    _destroyables.add(hasDestroy);
    return (T) hasDestroy;
  }

  public final void with(ZmqProcess process) {
    assert process != null;
    _threadPool.withProcess(process);
  }
}
