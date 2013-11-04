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

package org.zeromq.support.rpc.javanative;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.ObjectAdapter;

import java.io.IOException;

public final class JavanativeMsgProcessorOutputAdapter implements ObjectAdapter<Object[], ZmqMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(JavanativeMsgProcessorOutputAdapter.class);

  @Override
  public ZmqMessage convert(Object[] src) {
    Reply reply;
    Object invocResult = src[0];
    ZmqMessage message = (ZmqMessage) src[1];

    if (invocResult == null) {
      reply = Reply.VoidReply;
    }
    else if (Throwable.class.isAssignableFrom(invocResult.getClass())) {
      Throwable rc = Throwables.getRootCause((Throwable) invocResult);
      reply = new Reply(new Error(-1, rc));
    }
    else {
      try {
        reply = new Reply(JavanativeSerializationUtils.toBytes(invocResult));
      }
      catch (IOException e) {
        LOG.error("Got problem during invoc_result serialization: " + e, e);
        reply = new Reply(new Error(-1, e));
      }
    }

    try {
      return ZmqMessage.builder()
                       .withIdentities(message.identityFrames())
                       .withHeaders(message.headers())
                       .withPayload(JavanativeSerializationUtils.toBytes(reply))
                       .build();
    }
    catch (IOException e) {
      LOG.error("!!! Got problem during reply serialization: " + e, e);
      throw Throwables.propagate(e);
    }
  }
}
