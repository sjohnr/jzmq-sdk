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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

@SuppressWarnings("unchecked")
class Request implements Externalizable {

  private static final long serialVersionUID = Request.class.getName().hashCode();

  private String serviceName;
  private String functionName;
  private Object[] arguments = new Object[0];
  private Class<?>[] argumentClasses = new Class<?>[0];

  //// CONSTRUCTORS

  public Request() {
  }

  Request(String serviceName, String functionName, Object[] arguments, Class<?>[] argumentClasses) {
    this.serviceName = serviceName;
    this.functionName = functionName;
    this.arguments = arguments;
    this.argumentClasses = argumentClasses;
  }

  //// METHODS

  public Object[] arguments() {
    return arguments;
  }

  public Class<?>[] argumentClasses() {
    return argumentClasses;
  }

  public String functionName() {
    return functionName;
  }

  public String serviceName() {
    return serviceName;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(serviceName);
    out.writeUTF(functionName);
    int argumentsLen = arguments.length;
    out.writeInt(argumentsLen);
    for (Object argument : arguments) {
      boolean hasArg = argument != null;
      out.writeBoolean(hasArg);
      if (hasArg) {
        out.writeObject(argument);
      }
    }
    for (Class<?> argumentClass : argumentClasses) {
      out.writeObject(argumentClass);
    }
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    serviceName = in.readUTF();
    functionName = in.readUTF();
    int argumentsLen = in.readInt();
    arguments = new Object[argumentsLen];
    for (int i = 0; i < argumentsLen; i++) {
      boolean hasArg = in.readBoolean();
      arguments[i] = hasArg ? in.readObject() : null;
    }
    argumentClasses = new Class<?>[argumentsLen];
    for (int i = 0; i < argumentsLen; i++) {
      argumentClasses[i] = (Class<?>) in.readObject();
    }
  }
}
