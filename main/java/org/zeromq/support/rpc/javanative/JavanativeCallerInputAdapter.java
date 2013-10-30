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

public final class JavanativeCallerInputAdapter implements ObjectAdapter<ZmqMessage, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(JavanativeCallerInputAdapter.class);

  @Override
  public Object convert(ZmqMessage message) {
    Reply reply;
    try {
      reply = JavanativeSerializationUtils.fromBytes(message.payload());
    }
    catch (Exception e) {
      LOG.error("!!! Fatal error. Got problem during reply deserialization: " + e, e);
      throw Throwables.propagate(e);
    }
    switch (reply.replyType()) {
      case ERROR:
        String err =
            String.format("!!! Got invocation exception (ipc_reply_error [code=%s, message=\"%s\"]).",
                          reply.error().code(),
                          reply.error().message());
        LOG.error(err);
        throw new RuntimeException(err);
      case NORMAL:
        try {
          byte[] result = reply.result();
          return result.length != 0 ? JavanativeSerializationUtils.fromBytes(result) : null;
        }
        catch (Exception e) {
          LOG.error("!!! Fatal error. Got problem during result deserialization: " + e, e);
          throw Throwables.propagate(e);
        }
      default:
        LOG.error("!!! Fatal error. Unsupported ipc_reply_type detected: " + reply.replyType());
        throw new IllegalStateException();
    }
  }
}
