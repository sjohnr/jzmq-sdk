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

package org.zeromq.messaging;

import org.zeromq.support.ObjectBuilder;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Map data structure for <i>generic headers</i>:
 * <pre>
 *   header_id_0  header_value_0
 *   ...
 *   header_id_m  header_value_m
 * </pre>
 * Where header {@code ..._id and ..._value} are strings. Wire format as following:
 * <pre>
 *   header_id_0=header_value_0,...,header_id_m=header_value_m
 * </pre>
 */
public final class ZmqHeaders {

  private static final byte EQ = "=".getBytes()[0];
  private static final byte COMMA = ",".getBytes()[0];

  /** Outbound key-value string map. */
  private final Map<String, String> _map = new LinkedHashMap<String, String>();
  /** Helper attribute for optimal building binary format of {@link #_map}. */
  private int _mapCharCounter = 0;
  /** Inbound key-value comma separated string. */
  private byte[] _headers = new byte[0];

  public static final class Builder implements ObjectBuilder<ZmqHeaders> {

    private final ZmqHeaders _target = new ZmqHeaders();

    private Builder() {
      // no-op.
    }

    private Builder(byte[] headers) {
      checkArgument(headers != null);
      _target._headers = headers;
    }

    public Builder set(String key, String val) {
      checkArgument(key != null);
      checkArgument(val != null);

      String prev = _target._map.put(key, val);
      if (prev != null) {
        _target._mapCharCounter += (val.length() - prev.length());
      }
      else {
        _target._mapCharCounter += (key.length() + val.length());
      }

      return this;
    }

    @Override
    public ZmqHeaders build() {
      return _target;
    }
  }

  //// CONSTRUCTORS

  private ZmqHeaders() {
  }

  //// METHODS

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(byte[] headers) {
    return new Builder(headers);
  }

  /**
   * Getting outbound header value by header name.
   *
   * @param key header name.
   * @return null or header value from internal outbound {@link #_map}.
   */
  public String getHeader(String key) {
    checkArgument(key != null);
    return _map.get(key);
  }

  /**
   * Getting inbound header value by header name.
   *
   * @param key header name.
   * @return null or header value from internal inbound key-value {@link #_headers}.
   */
  public String getHeader(byte[] key) {
    int start = -1;
    int headersLen = _headers.length;
    int keyLen = key.length;
    for (int h = 0; h < headersLen && h + keyLen < headersLen; ) {
      // check edges.
      byte left = _headers[h];
      byte right = _headers[h + keyLen - 1];
      if (left == key[0] && right == key[keyLen - 1]) {
        int k_ind = 0;
        int h_ind = h;
        // if edges matched check bytes between.
        for (; k_ind < keyLen; ) {
          if (key[k_ind++] != _headers[h_ind++]) {
            break;
          }
        }
        // check that "=" was matched and match beginning of the value.
        if (k_ind == keyLen) {
          checkState(EQ == _headers[h_ind]);
          start = h_ind + 1;
          break;
        }
      }
      // progress header index until next "," or the end of string.
      for (;h < headersLen;) {
        if (COMMA == _headers[h++]) {
          break;
        }
      }
    }
    if (start == -1) {
      return null;
    }
    if (start == headersLen) {
      return "";
    }
    // find "," or the end of string.
    int end = headersLen;
    for (int i = start; i < end; i++) {
      if (COMMA == _headers[i]) {
        end = i;
        break;
      }
    }
    return new String(Arrays.copyOfRange(_headers, start, end));
  }

  public byte[] asBinary() {
    int headersLen = _headers.length;
    int binLen = headersLen;

    if (headersLen > 0 && _mapCharCounter > 0) {
      binLen++; // ","
    }

    byte[] mapBuf = new byte[0];
    if (_mapCharCounter > 0) {
      Set<Map.Entry<String, String>> entries = _map.entrySet();
      int mapSize = entries.size();
      CharBuffer buffer = CharBuffer.allocate(_mapCharCounter + mapSize /*=*/ + mapSize - 1 /*,*/);
      int i = 0;
      for (Map.Entry<String, String> entry : entries) {
        buffer.put(entry.getKey());
        buffer.put("=");
        buffer.put(entry.getValue());
        if (i != mapSize - 1) {
          buffer.put(",");
        }
        i++;
      }
      mapBuf = Charset.forName("ISO-8859-1").encode((CharBuffer) buffer.flip()).array();
      binLen += mapBuf.length;
    }

    byte[] binBuf = new byte[binLen];
    if (headersLen > 0) {
      System.arraycopy(_headers, 0, binBuf, 0, headersLen);
    }
    if (mapBuf.length > 0) {
      int destPos = headersLen;
      if (headersLen > 0) {
        binBuf[headersLen] = COMMA;
        destPos++;
      }
      System.arraycopy(mapBuf, 0, binBuf, destPos, mapBuf.length);
    }

    return binBuf;
  }
}
