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

package org.zeromq.support.rpc;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.support.HasInit;
import org.zeromq.support.ObjectAdapter;
import org.zeromq.support.ObjectBuilder;
import org.zeromq.support.pool.Lease;
import org.zeromq.support.pool.ObjectPool;
import org.zeromq.support.pool.SimpleObjectPool;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.transform;
import static java.util.Arrays.asList;

public final class ZmqRpcCaller implements MethodInterceptor, HasInit {

  private static final int DEFAULT_NUM_OF_CALLERS = 2;
  private static final long DEFAULT_CALLER_LEASE_TIMEOUT = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(ZmqRpcCaller.class);

  protected ZmqContext zmqContext;
  protected List<String> connectAddresses = new ArrayList<String>();
  protected String identity;
  protected ObjectAdapter<String, byte[]> identityConverter = new ObjectAdapter<String, byte[]>() {
    @Override
    public byte[] convert(String src) {
      return src.getBytes();
    }
  };
  protected int numOfCallers = DEFAULT_NUM_OF_CALLERS;
  protected long callerLeaseTimeout = DEFAULT_CALLER_LEASE_TIMEOUT;
  protected ObjectAdapter<MethodInvocation, ZmqMessage> outputAdapter;
  protected ObjectAdapter<ZmqMessage, Object> inputAdapter;

  protected ObjectPool<ZmqChannel> _channelPool;

  //// METHODS

  public void setZmqContext(ZmqContext zmqContext) {
    this.zmqContext = zmqContext;
  }

  public void setConnectAddresses(List<String> connectAddresses) {
    this.connectAddresses = connectAddresses;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public void setIdentityConverter(ObjectAdapter<String, byte[]> identityConverter) {
    this.identityConverter = identityConverter;
  }

  public void setNumOfCallers(int numOfCallers) {
    if (numOfCallers > 0) {
      this.numOfCallers = numOfCallers;
    }
  }

  public void setCallerLeaseTimeout(long callerLeaseTimeout) {
    if (callerLeaseTimeout > 0) {
      this.callerLeaseTimeout = callerLeaseTimeout;
    }
  }

  public void setOutputAdapter(ObjectAdapter<MethodInvocation, ZmqMessage> outputAdapter) {
    this.outputAdapter = outputAdapter;
  }

  public void setInputAdapter(ObjectAdapter<ZmqMessage, Object> inputAdapter) {
    this.inputAdapter = inputAdapter;
  }

  @Override
  public void init() {
    if (zmqContext == null) {
      throw ZmqException.fatal();
    }
    if (connectAddresses.isEmpty()) {
      throw ZmqException.fatal();
    }
    if (outputAdapter == null) {
      throw ZmqException.fatal();
    }
    if (inputAdapter == null) {
      throw ZmqException.fatal();
    }
    if (identity != null) {
      if (identityConverter == null) {
        throw ZmqException.fatal();
      }
    }

    ObjectBuilder<ZmqChannel> clientBuilder = new ObjectBuilder<ZmqChannel>() {
      @Override
      public void checkInvariant() {
        // no-op.
      }

      @Override
      public ZmqChannel build() {
        ZmqChannel.Builder builder = ZmqChannel.builder();
        if (identity != null) {
          builder.withSocketIdentityPrefix(identityConverter.convert(identity));
        }
        return builder.withZmqContext(zmqContext)
                      .ofDEALERType()
                      .withConnectAddresses(connectAddresses)
                      .build();
      }
    };

    _channelPool = numOfCallers > 0 ?
                   new SimpleObjectPool<ZmqChannel>(numOfCallers, clientBuilder) :
                   new SimpleObjectPool<ZmqChannel>(clientBuilder);
  }

  @Override
  public Object invoke(MethodInvocation invocation) {
    Method method = invocation.getMethod();
    String serviceName = method.getDeclaringClass().getCanonicalName();
    String functionName = method.getName();

    if (LOG.isTraceEnabled()) {
      String argumentsLoggable = Joiner.on(", ")
                                       .join(transform(asList(method.getParameterTypes()),
                                                       new Function<Class<?>, String>() {
                                                         @Override
                                                         public String apply(Class<?> clazz) {
                                                           return clazz.getCanonicalName();
                                                         }
                                                       }));
      LOG.trace("Got invocation {}.{}({}). Passing further.", serviceName, functionName, argumentsLoggable);
    }

    ZmqMessage request;
    try {
      request = outputAdapter.convert(invocation);
    }
    catch (Exception e) {
      throw ZmqException.seeCause(e);
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Converted method_invocation -> zmq_request ({} b).", request.payload().length);
    }

    Lease<ZmqChannel> l = _channelPool.lease(callerLeaseTimeout);
    if (l == null) {
      LOG.error("!!! Can't lease channel from channel_pool (waited {} ms).", callerLeaseTimeout);
      throw ZmqException.failedAtSend();
    }

    ZmqChannel caller = l.get();
    ZmqMessage reply;
    try {
      boolean sent = caller.send(request);
      if (!sent) {
        throw ZmqException.failedAtSend();
      }
      reply = caller.recv();
      if (reply == null) {
        throw ZmqException.failedAtRecv();
      }
    }
    finally {
      l.release();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Got zmq_reply ({} b).", reply.payload().length);
    }

    Object result;
    try {
      result = inputAdapter.convert(reply);
    }
    catch (Exception e) {
      throw ZmqException.seeCause(e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Converted zmq_reply -> object ({} b). Returning.", reply.payload().length);
    }
    return result;
  }
}
