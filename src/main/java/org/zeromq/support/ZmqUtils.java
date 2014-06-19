package org.zeromq.support;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeromq.messaging.ZmqException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;

public class ZmqUtils {

  private ZmqUtils() {
  }

  public static boolean isEmptyFrame(byte[] frame) {
    checkArgument(frame != null);
    return frame.length == 0;
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
      throw ZmqException.seeCause(e);
    }
  }
}
