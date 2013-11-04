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
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.ObjectAdapter;

import java.io.IOException;
import java.lang.reflect.Method;

public final class JavanativeCallerOutputAdapter implements ObjectAdapter<MethodInvocation, ZmqMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(JavanativeCallerOutputAdapter.class);

  @Override
  public ZmqMessage convert(MethodInvocation invocation) {
    Method method = invocation.getMethod();
    String serviceName = method.getDeclaringClass().getCanonicalName();
    String functionName = method.getName();
    Class<?>[] argClasses = method.getParameterTypes();
    Object[] args = invocation.getArguments();
    try {
      byte[] payload = JavanativeSerializationUtils.toBytes(new Request(serviceName, functionName, args, argClasses));
      return ZmqMessage.builder().withPayload(payload).build();
    }
    catch (IOException e) {
      LOG.error("!!! Got problem during request serialization: " + e, e);
      throw Throwables.propagate(e);
    }
  }
}
