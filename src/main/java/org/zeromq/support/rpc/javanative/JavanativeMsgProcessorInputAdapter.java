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
import org.zeromq.support.rpc.ZmqRpcMsgProcessor;

public final class JavanativeMsgProcessorInputAdapter
    implements ObjectAdapter<ZmqMessage, ZmqRpcMsgProcessor.Call> {

  private static final Logger LOG = LoggerFactory.getLogger(JavanativeMsgProcessorInputAdapter.class);

  private static class CallImpl implements ZmqRpcMsgProcessor.Call {

    final Request request;

    CallImpl(Request request) {
      this.request = request;
    }

    @Override
    public String serviceName() {
      return request.serviceName();
    }

    @Override
    public String functionName() {
      return request.functionName();
    }

    @Override
    public Class<?>[] argClasses() {
      return request.argumentClasses();
    }

    @Override
    public Object[] args() {
      return request.arguments();
    }
  }

  @Override
  public ZmqRpcMsgProcessor.Call convert(ZmqMessage message) {
    try {
      Request request = JavanativeSerializationUtils.fromBytes(message.payload());
      return new CallImpl(request);
    }
    catch (Exception e) {
      LOG.error("!!! Fatal error. Got problem during request deserialization: " + e, e);
      throw Throwables.propagate(e);
    }
  }
}
