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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;
import org.zeromq.messaging.device.service.ZmqMessageProcessor;
import org.zeromq.support.ObjectAdapter;

import java.lang.reflect.Method;

import static com.google.common.collect.Lists.transform;
import static java.util.Arrays.asList;

public final class ZmqRpcMsgProcessor implements ZmqMessageProcessor, ApplicationContextAware {

  private static final Logger LOG = LoggerFactory.getLogger(ZmqRpcMsgProcessor.class);

  public static interface Call {

    String serviceName();

    String functionName();

    Class<?>[] argClasses();

    Object[] args();
  }

  private ApplicationContext applicationContext;
  private ObjectAdapter<ZmqMessage, Call> inputAdapter;
  private ObjectAdapter<Object[], ZmqMessage> outputAdapter;

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) {
    this.applicationContext = applicationContext;
  }

  public void setInputAdapter(ObjectAdapter<ZmqMessage, Call> inputAdapter) {
    this.inputAdapter = inputAdapter;
  }

  public void setOutputAdapter(ObjectAdapter<Object[], ZmqMessage> outputAdapter) {
    this.outputAdapter = outputAdapter;
  }

  @Override
  public ZmqMessage process(ZmqMessage message) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Got zmq_request ({} b). Processing.", message.payload().length);
    }

    Object invocResult;
    String serviceName = null;
    String functionName = null;
    String arguments = null;
    try {
      Call call = inputAdapter.convert(message);

      serviceName = call.serviceName();
      functionName = call.functionName();

      Object serviceBean = applicationContext.getBean(serviceName);
      Class<?>[] argClasses = call.argClasses();
      Method method = serviceBean.getClass().getDeclaredMethod(functionName, argClasses);

      if (LOG.isTraceEnabled()) {
        arguments = Joiner.on(", ")
                          .join(transform(asList(argClasses),
                                          new Function<Class<?>, String>() {
                                            @Override
                                            public String apply(Class<?> clazz) {
                                              return clazz.getCanonicalName();
                                            }
                                          }));
        LOG.trace("Invoking {}.{}({}) ...", serviceName, functionName, arguments);
      }

      invocResult = method.invoke(serviceBean, call.args());

      if (LOG.isTraceEnabled()) {
        LOG.trace("Got invoc_result ({}) on {}.{}({}).", invocResult, serviceName, functionName, arguments);
      }
    }
    catch (Exception e) {
      if (serviceName != null && functionName != null && arguments != null) {
        LOG.warn("Got exception at invoking " +
                 serviceName + "." + functionName + "(" + arguments + "). " +
                 "Set invoc_result to: " + e, e);
      }
      else if (serviceName != null && functionName != null) {
        LOG.warn("Got exception at invoking " +
                 serviceName + "." + functionName + ". " +
                 "Set invoc_result to: " + e, e);
      }
      else {
        LOG.warn("Set invoc_result to: " + e, e);
      }
      invocResult = e;
    }

    ZmqMessage reply;
    try {
      reply = outputAdapter.convert(new Object[]{invocResult, message});
    }
    catch (Exception e) {
      throw ZmqException.seeCause(e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Converted invoc_result -> zmq_reply ({} b). Returning.", reply.payload().length);
    }
    return reply;
  }
}
