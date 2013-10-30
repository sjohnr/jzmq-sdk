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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class JavanativeSerializationUtils {

  private JavanativeSerializationUtils() {
  }

  public static byte[] toBytes(Object object) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = null;
    byte[] bytes = null;
    try {
      oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
      oos.flush();
      bytes = baos.toByteArray();
    }
    finally {
      _closeQuietly(oos);
    }
    return bytes;
  }

  @SuppressWarnings("unchecked")
  public static <T> T fromBytes(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = null;
    T t = null;
    try {
      ois = new ObjectInputStream(bais);
      t = (T) ois.readObject();
    }
    finally {
      _closeQuietly(ois);
    }
    return t;
  }

  private static void _closeQuietly(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      }
      catch (IOException ignore) {
      }
    }
  }
}
