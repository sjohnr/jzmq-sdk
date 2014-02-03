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

package org.zeromq.messaging;

import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;
import org.zeromq.support.thread.ThreadPool;
import org.zeromq.support.thread.ZmqRunnable;

import java.util.ArrayList;
import java.util.List;

public class BaseFixture implements HasInit, HasDestroy {

  private final List<HasDestroy> _destroyables = new ArrayList<HasDestroy>();
  private final ThreadPool _threadPool = ThreadPool.newCachedDaemonThreadPool();

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

  public final void with(ZmqRunnable r) {
    assert r != null;
    _threadPool.withRunnable(r);
  }
}
