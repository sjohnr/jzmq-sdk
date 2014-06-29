package org.zeromq.support;

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

  public static long makeHash(byte[] buf) {
    CRC32 hashFunction = new CRC32();
    hashFunction.update(buf);
    return hashFunction.getValue();
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
}
