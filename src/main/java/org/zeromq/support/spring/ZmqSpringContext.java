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

package org.zeromq.support.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.zeromq.messaging.ZmqException;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.IsPrototype;
import org.zeromq.support.thread.ZmqRunnable;
import org.zeromq.support.thread.ZmqRunnableContext;
import org.zeromq.support.thread.ZmqThreadPool;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class ZmqSpringContext implements HasDestroy {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqSpringContext.class);

  private final AbstractApplicationContext applicationContext;

  private final ZmqThreadPool _threadPool = ZmqThreadPool.newCachedNonDaemonThreadPool();

  //// CONSTRUCTORS

  public ZmqSpringContext(AbstractApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  //// METHODS

  public void deploy() {
    Thread shutdownHook = new Thread() {
      @Override
      public void run() {
        try {
          applicationContext.destroy();
        }
        catch (Throwable ignore) {
        }
      }
    };
    shutdownHook.setDaemon(true);
    shutdownHook.setName("zmq-shutdown-hook");
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    boolean includeNonSingletons = true;
    boolean allowEagerInit = false;
    Map<String, ZmqRunnableContext> beans =
        applicationContext.getBeansOfType(ZmqRunnableContext.class, includeNonSingletons, allowEagerInit);

    if (beans.isEmpty()) {
      LOG.error("!!! Not a single runnable_context registered.");
      throw ZmqException.fatal();
    }

    for (Map.Entry<String, ZmqRunnableContext> entry : beans.entrySet()) {
      ZmqRunnableContext runnableContext = entry.getValue();
      Class<? extends ZmqRunnableContext> clazz = runnableContext.getClass();

      if (IsPrototype.class.isAssignableFrom(clazz)) {
        int numOfCopies = ((IsPrototype) runnableContext).numOfCopies();
        Set<ZmqRunnableContext> prototypes = new HashSet<ZmqRunnableContext>();
        for (int i = 0; i < numOfCopies; i++) {
          String key = entry.getKey();
          ZmqRunnableContext prototype = applicationContext.getBean(key, ZmqRunnableContext.class);
          if (!prototypes.add(prototype)) {
            LOG.error("!!! Not a prototype scoped runnable_context detected: {}.", key);
            throw ZmqException.fatal();
          }
          prototypes.add(prototype);
          deploy(prototype);
        }
      }
      else {
        deploy(runnableContext);
      }
    }

    _threadPool.init();
  }

  private void deploy(ZmqRunnableContext runnableContext) {
    _threadPool.withRunnable(
        ZmqRunnable.builder()
                   .withRunnableContext(runnableContext)
                   .build()
    );
  }

  @Override
  public void destroy() {
    _threadPool.destroy();
    applicationContext.destroy();
  }
}
