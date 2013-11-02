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

package com.google.common.eventbus;

import org.zeromq.messaging.ZmqException;

import java.lang.reflect.InvocationTargetException;

/**
 * Extension of Google' {@link EventBus}.
 * The only difference -- doesn't gobble InvocationTargetException inside
 * {@link #dispatch(Object, EventHandler)} method, but wraps it and re-throws.
 */
public class ZmqEventBus extends EventBus {

  public ZmqEventBus() {
  }

  public ZmqEventBus(String identifier) {
    super(identifier);
  }

  @Override
  protected void dispatch(Object event, EventHandler wrapper) {
    try {
      wrapper.handleEvent(event);
    }
    catch (InvocationTargetException e) {
      throw ZmqException.seeCause(e.getCause());
    }
  }
}
