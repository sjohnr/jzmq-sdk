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

import org.zeromq.messaging.extension.TryAgainEventListener;
import org.zeromq.support.HasDestroy;
import org.zeromq.support.HasInit;
import org.zeromq.support.thread.ZmqThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;

public abstract class ZmqAbstractArchitectureTest extends ZmqAbstractTest {

  protected static class Fixture extends ZmqAbstractTest.Fixture implements HasInit, HasDestroy {

    protected final List<HasDestroy> _destroyables = new ArrayList<HasDestroy>();
    protected final ZmqThreadPool _threadPool = ZmqThreadPool.newCachedDaemonThreadPool();

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

    public ZmqChannel newBindingClient(ZmqContext zmqContext, String bindAddress) {
      ZmqChannel target = ZmqChannelFactory.builder()
                                           .withZmqContext(zmqContext)
                                           .ofDEALERType()
                                           .withBindAddress(bindAddress)
                                           .build()
                                           .newChannel();

      _destroyables.add(target);
      return target;
    }

    public ZmqChannel newConnClient(ZmqContext zmqContext, String... connAddresses) {
      ZmqChannel target = ZmqChannelFactory.builder()
                                           .withZmqContext(zmqContext)
                                           .ofDEALERType()
                                           .withConnectAddresses(Arrays.asList(connAddresses))
                                           .withEventListener(new TryAgainEventListener())
                                           .build()
                                           .newChannel();
      _destroyables.add(target);
      return target;
    }

    public ZmqChannel newConnClientWithIdentity(ZmqContext zmqContext, String identityPrefix, String... connAddresses) {
      ZmqChannel target = ZmqChannelFactory.builder()
                                           .withZmqContext(zmqContext)
                                           .ofDEALERType()
                                           .withConnectAddresses(Arrays.asList(connAddresses))
                                           .withSocketIdentityPrefix(identityPrefix.getBytes())
                                           .withEventListener(new TryAgainEventListener())
                                           .build()
                                           .newChannel();
      _destroyables.add(target);
      return target;
    }
  }

  protected void assertPayload(String expectedPayload, ZmqMessage message) {
    assert message != null;
    assert message.payload() != null;
    assertEquals(expectedPayload, new String(message.payload()));
  }
}
