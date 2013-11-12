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

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessage;
import org.aopalliance.intercept.MethodInvocation;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.service.ServiceHeaders;
import org.zeromq.support.ObjectAdapter;

import java.util.UUID;

public final class ProtoCallerOutputAdapter implements ObjectAdapter<MethodInvocation, ZmqMessage> {

  @Override
  public ZmqMessage convert(MethodInvocation invocation) {
    Object[] arguments = invocation.getArguments();
    Preconditions.checkArgument(arguments.length == 1);
    GeneratedMessage protoMessage = (GeneratedMessage) arguments[0];

    String serviceName = invocation.getMethod().getDeclaringClass().getCanonicalName();

    byte[] payload = Proto.Request.newBuilder()
                          .setServiceName(serviceName)
                          .setFunctionName(invocation.getMethod().getName())
                          .setArgumentType(protoMessage.getClass().getName())
                          .setArgument(protoMessage.toByteString())
                          .build()
                          .toByteArray();

    long corrId = UUID.randomUUID().getMostSignificantBits();
    return ZmqMessage.builder()
                     .withHeaders(new ServiceHeaders().setCorrId(corrId))
                     .withPayload(payload)
                     .build();
  }
}
