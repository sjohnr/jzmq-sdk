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

package org.zeromq.support.rpc.proto;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.ObjectAdapter;

import java.lang.reflect.Method;

public final class ProtoCallerInputAdapter implements ObjectAdapter<ZmqMessage, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoCallerInputAdapter.class);

  @SuppressWarnings("unchecked")
  @Override
  public Object convert(ZmqMessage message) {
    Proto.Reply reply;
    try {
      reply = Proto.Reply.parseFrom(message.payload());
    }
    catch (InvalidProtocolBufferException e) {
      LOG.error("!!! Got problem during result deserialization: " + e, e);
      throw Throwables.propagate(e);
    }
    switch (reply.getReplyType()) {
      case ERROR:
        String err =
            String.format("!!! Got invocation exception (ipc_reply_error [code=%s, message=\"%s\"]).",
                          reply.getError().getCode(),
                          reply.getError().getMessage());
        LOG.error(err);
        throw new RuntimeException(err);
      case NORMAL:
        String resultType = reply.getResultType();
        if (resultType.equals("void")) {
          return null;
        }
        try {
          Class<GeneratedMessage> protoMessageClass = (Class<GeneratedMessage>) Class.forName(resultType);
          Method method = protoMessageClass.getMethod("parseFrom", new Class[]{ByteString.class});
          return method.invoke(null, reply.getResult());
        }
        catch (Exception e) {
          LOG.error("!!! Got problem during result deserialization: " + e, e);
          throw Throwables.propagate(e);
        }
      default:
        LOG.error("!!! Unsupported ipc_reply_type detected: " + reply.getReplyType());
        throw new IllegalStateException();
    }
  }
}
