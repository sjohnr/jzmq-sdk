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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.rpc.ZmqRpcMsgProcessor;

import java.lang.reflect.Method;

public final class ProtoMsgProcessorInputAdapter
    implements ObjectAdapter<ZmqMessage, ZmqRpcMsgProcessor.Call> {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoMsgProcessorInputAdapter.class);

  private static class CallImpl implements ZmqRpcMsgProcessor.Call {

    final Proto.Request request;

    CallImpl(Proto.Request request) {
      this.request = request;
    }

    @Override
    public String serviceName() {
      return request.getServiceName();
    }

    @Override
    public String functionName() {
      return request.getFunctionName();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Class<GeneratedMessage>[] argClasses() {
      try {
        Class<GeneratedMessage> protoMessageClass =
            (Class<GeneratedMessage>) Class.forName(request.getArgumentType());
        return new Class[]{protoMessageClass};
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public Object[] args() {
      try {
        Class<GeneratedMessage>[] argClasses = argClasses();
        Method method = argClasses[0].getMethod("parseFrom", new Class[]{ByteString.class});
        GeneratedMessage args = (GeneratedMessage) method.invoke(null, request.getArgument());
        return new Object[]{args};
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public ZmqRpcMsgProcessor.Call convert(ZmqMessage message) {
    try {
      Proto.Request request = Proto.Request.parseFrom(message.payload());
      return new CallImpl(request);
    }
    catch (Exception e) {
      LOG.error("!!! Got problem during request deserialization: " + e, e);
      throw Throwables.propagate(e);
    }
  }
}
