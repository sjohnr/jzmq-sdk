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

package org.zeromq.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeromq.messaging.ZmqException;
import org.zeromq.messaging.ZmqMessage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.CRC32;

public class ZmqUtils {

  private ZmqUtils() {
  }

  public static boolean isEmptyFrame(byte[] frame) {
    assert frame != null;
    return frame.length == 0;
  }

  public static boolean isDivFrame(byte[] frame) {
    assert frame != null;
    return Arrays.equals(ZmqMessage.DIV_FRAME, frame);
  }

  public static long makeHash(Iterable<byte[]> buf) {
    CRC32 hashFunction = new CRC32();
    for (byte[] b : buf) {
      hashFunction.update(b);
    }
    return hashFunction.getValue();
  }

  public static byte[] longAsBytes(long l) {
    return ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array();
  }

  public static long bytesAsLong(byte[] buf) {
    ByteBuffer bbuf = ByteBuffer.allocate(Long.SIZE / 8).put(buf);
    bbuf.flip();
    return bbuf.getLong();
  }

  public static byte[] intAsBytes(int i) {
    return ByteBuffer.allocate(Integer.SIZE / 8).putInt(i).array();
  }

  public static int bytesAsInt(byte[] buf) {
    ByteBuffer bbuf = ByteBuffer.allocate(Integer.SIZE / 8).put(buf);
    bbuf.flip();
    return bbuf.getInt();
  }

  public static byte[] mergeBytes(Iterable<byte[]> bufs) {
    int totalLen = 0;
    for (byte[] buf : bufs) {
      totalLen += buf.length;
    }
    byte[] target = new byte[totalLen];
    int pos = 0;
    for (byte[] buf : bufs) {
      System.arraycopy(buf, 0, target, pos, buf.length);
      pos += buf.length;
    }
    return target;
  }

  public static String mapAsJson(Map<String, Object> map) {
    try {
      return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(map);
    }
    catch (JsonProcessingException e) {
      throw ZmqException.wrap(e);
    }
  }
}
