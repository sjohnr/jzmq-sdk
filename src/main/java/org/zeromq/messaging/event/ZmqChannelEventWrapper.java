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

package org.zeromq.messaging.event;

import com.google.common.base.Stopwatch;
import com.google.common.eventbus.ZmqEventBus;
import org.zeromq.ZMQ;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.event.NoOpEventListener;

/**
 * Message channel with events.
 * <p/>
 * On {@code .send()} following events being emitted:
 * <ul>
 * <li>{@link EventBeforeSend}</li>
 * <li>{@link EventAfterSend}</li>
 * <li>{@link EventSent}</li>
 * <li>{@link EventNotSent}</li>
 * </ul>
 * On {@code .recv()} following events being emitted:
 * <ul>
 * <li>{@link EventBeforeReceive}</li>
 * <li>{@link EventAfterReceive}</li>
 * <li>{@link EventReceived}</li>
 * <li>{@link EventNotReceived}</li>
 * </ul>
 * On {@code .subscribe()/.unsubscribe()} following events being emitted:
 * <ul>
 * <li>{@link EventBeforeSubscribe}</li>
 * <li>{@link EventAfterSubscribe}</li>
 * <li>{@link EventSubscribed}</li>
 * <li>{@link EventBeforeUnsubscribe}</li>
 * <li>{@link EventAfterUnsubscribe}</li>
 * <li>{@link EventUnsubscribed}</li>
 * </ul>
 */
public final class ZmqChannelEventWrapper implements ZmqChannel {

  private final ZmqChannel channel;
  private final ZmqEventBus eventBus = new ZmqEventBus();

  //// CONSTRUCTORS

  public ZmqChannelEventWrapper(ZmqChannel channel, Iterable eventListeners) {
    this.channel = channel;
    eventBus.register(new NoOpEventListener());
    for (Object el : eventListeners) {
      eventBus.register(el);
    }
  }

  //// METHODS

  @Override
  public final void destroy() {
    channel.destroy();
  }

  @Override
  public final void register(ZMQ.Poller poller) {
    channel.register(poller);
  }

  @Override
  public final void unregister() {
    channel.unregister();
  }

  @Override
  public final boolean hasInput() {
    return channel.hasInput();
  }

  @Override
  public final boolean hasOutput() {
    return channel.hasOutput();
  }

  @Override
  public boolean send(ZmqMessage message) throws ZmqException {
    Throwable t = null;
    try {
      eventBus.post(new EventBeforeSend(channel, message));

      Stopwatch timer = new Stopwatch().start();
      boolean sent = channel.send(message);
      long elapsedTime = timer.stop().elapsedMillis();

      if (sent) {
        eventBus.post(new EventSent(channel, message));
      }
      else {
        eventBus.post(new EventNotSent(channel, message, elapsedTime));
      }
      return sent;
    }
    catch (ZmqException e) {
      t = e;
      throw e;
    }
    catch (Exception e) {
      t = e;
      throw ZmqException.wrap(e);
    }
    finally {
      eventBus.post(new EventAfterSend(t));
    }
  }

  @Override
  public ZmqMessage recv() throws ZmqException {
    Throwable t = null;
    try {
      eventBus.post(new EventBeforeReceive(channel));

      Stopwatch timer = new Stopwatch().start();
      ZmqMessage message = channel.recv();
      long elapsedTime = timer.stop().elapsedMillis();

      if (message == null) {
        eventBus.post(new EventNotReceived(channel, elapsedTime));
        return null;
      }
      else {
        EventReceived event = new EventReceived(channel, message, elapsedTime);
        eventBus.post(event);
        message = event.message();
        if (message == null) {
          eventBus.post(new EventNotReceived(channel, elapsedTime));
          return null;
        }
      }
      return message;
    }
    catch (ZmqException e) {
      t = e;
      throw e;
    }
    catch (Exception e) {
      t = e;
      throw ZmqException.wrap(e);
    }
    finally {
      eventBus.post(new EventAfterReceive(t));
    }
  }

  @Override
  public void subscribe(byte[] topic) {
    Throwable t = null;
    try {
      eventBus.post(new EventBeforeSubscribe(channel, topic));
      channel.subscribe(topic);
      eventBus.post(new EventSubscribed(channel, topic));
    }
    catch (ZmqException e) {
      t = e;
      throw e;
    }
    catch (Exception e) {
      t = e;
      throw ZmqException.wrap(e);
    }
    finally {
      eventBus.post(new EventAfterSubscribe(t));
    }
  }

  @Override
  public void unsubscribe(byte[] topic) {
    Throwable t = null;
    try {
      eventBus.post(new EventBeforeUnsubscribe(channel, topic));
      channel.unsubscribe(topic);
      eventBus.post(new EventUnsubscribed(channel, topic));
    }
    catch (ZmqException e) {
      t = e;
      throw e;
    }
    catch (Throwable e) {
      t = e;
      throw ZmqException.wrap(e);
    }
    finally {
      eventBus.post(new EventAfterUnsubscribe(t));
    }
  }
}
