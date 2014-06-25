package org.zeromq.support;

import org.zeromq.messaging.ZmqFrames;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.messaging.ZmqFrames.BYTE_SUB;
import static org.zeromq.messaging.ZmqFrames.BYTE_UNSUB;
import static org.zeromq.messaging.ZmqFrames.EMPTY_FRAME;

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

  public static byte getExtPubSub(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() == 1, "Wrong frames.size: " + frames.size());

    byte b = frames.get(0)[0];
    checkArgument(BYTE_SUB == b || BYTE_UNSUB == b, "Unrecognized ExtPubSub byte: " + b);

    return b;
  }

  public static byte[] getExtPubSubTopic(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() == 1, "Wrong frames.size: " + frames.size());

    byte[] buf = frames.get(0);
    return buf.length > 1 ? Arrays.copyOfRange(buf, 1, buf.length) : EMPTY_FRAME;
  }

  public static byte[] getTopic(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() > 0, "Wrong frames.size: " + frames.size());

    return frames.get(0);
  }

  public static ZmqFrames getIdentities(ZmqFrames frames) {
    checkArgument(frames != null);

    ZmqFrames identities = new ZmqFrames();
    Iterator<byte[]> iterator = frames.iterator();
    int emptyFrameSeen = 0;
    for (; iterator.hasNext(); ) {
      byte[] frame = iterator.next();
      if (isEmptyFrame(frame)) {
        ++emptyFrameSeen;
      }
      if (emptyFrameSeen == 2) {
        break;
      }
      if (!isEmptyFrame(frame)) {
        emptyFrameSeen = 0;
        identities.add(frame);
      }
    }
    return identities;
  }

  public static byte[] getHeaders(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() > 0, "Wrong frames.size: " + frames.size());

    ByteBuffer buf = ByteBuffer.wrap(frames.get(frames.size() - 1));
    byte[] headers = new byte[buf.getInt()];
    buf.get(headers);
    return headers;
  }

  public static byte[] getPayload(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() > 0, "Wrong frames.size: " + frames.size());

    ByteBuffer buf = ByteBuffer.wrap(frames.get(frames.size() - 1));
    int headersLen = buf.getInt();
    buf.position(4 + headersLen);

    byte[] payload = new byte[buf.getInt()];
    buf.get(payload);
    return payload;
  }

  public static int getInprocRef(ZmqFrames frames) {
    checkArgument(frames != null);
    checkArgument(frames.size() > 0, "Wrong frames.size: " + frames.size());

    byte[] buf = frames.get(frames.size() - 1);
    checkArgument(buf.length == 4);
    return ((buf[0] & 0xFF) << 24) | ((buf[1] & 0xFF) << 16) | ((buf[2] & 0xFF) << 8) | buf[3] & 0xFF;
  }
}
