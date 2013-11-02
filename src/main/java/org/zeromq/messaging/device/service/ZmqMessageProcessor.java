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

package org.zeromq.messaging.device.service;

import org.zeromq.messaging.ZmqMessage;

/**
 * Entry point for processing incoming {@link ZmqMessage} on behalf of {@link ZmqAbstractWorker}.
 * <p/>
 * Client implementations should encapsulte parsing of incoming {@link ZmqMessage},
 * deserializing his payload and passing object down in the call chain.
 * Reverse process is similar: object should be converted back to the
 * {@link ZmqMessage} message.
 * <p/>
 * <b>NOTE: returning back reply message is optional.</b>
 */
public interface ZmqMessageProcessor {

  /**
   * {@link org.zeromq.messaging.ZmqMessage} processor function.
   *
   * @param message incoming request_message.
   * @return reply_message or {@code null} if reply is not required.
   */
  ZmqMessage process(ZmqMessage message);
}
