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
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.ObjectAdapter;

import static org.zeromq.support.rpc.proto.Proto.Reply;

public final class ProtoMsgProcessorOutputAdapter implements ObjectAdapter<Object[], ZmqMessage> {

  @Override
  public ZmqMessage convert(Object[] src) {
    Reply reply;
    Object invocResult = src[0];
    ZmqMessage message = (ZmqMessage) src[1];

    if (invocResult == null) {
      reply = Reply.newBuilder()
                   .setReplyType(Reply.Type.NORMAL)
                   .setResultType("void")
                   .setResult(ByteString.EMPTY)
                   .build();
    }
    else if (Throwable.class.isAssignableFrom(invocResult.getClass())) {
      Throwable rc = Throwables.getRootCause((Throwable) invocResult);
      reply = Reply.newBuilder()
                   .setReplyType(Reply.Type.ERROR)
                   .setError(Reply.Error.newBuilder()
                                  .setCode(-1)
                                  .setMessage(rc.getMessage())
                                  .build())
                   .build();
    }
    else {
      GeneratedMessage protoResult = (GeneratedMessage) invocResult;
      reply = Reply.newBuilder()
                   .setReplyType(Reply.Type.NORMAL)
                   .setResultType(invocResult.getClass().getName())
                   .setResult(protoResult.toByteString())
                   .build();
    }

    return ZmqMessage.builder()
                     .withIdentities(message.identityFrames())
                     .withHeaders(message.headers())
                     .withPayload(reply.toByteArray())
                     .build();
  }
}
