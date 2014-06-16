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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

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
  private byte[] _headers;

  public static final class Builder implements ObjectBuilder<ZmqHeaders> {

    private final ZmqHeaders _target = new ZmqHeaders();

    private Builder() {
      // no-op.
    }

    private Builder(byte[] headers) {
      checkArgument(headers != null);
      _target._headers = headers;
    }

    public Builder set(String k, String v) {
      checkArgument(k != null);
      checkArgument(v != null);

      String prev = _target._map.put(k, v);
      if (prev != null) {
        _target._mapCharCounter += (v.length() - prev.length());
      }
      else {
        _target._mapCharCounter += (k.length() + v.length());
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
   * @param k header name.
   * @return null or header value from internal outbound {@link #_map}.
   */
  public String getHeader(String k) {
    checkArgument(k != null);
    return _map.get(k);
  }

  /**
   * Getting inbound header value by header name.
   *
   * @param k header name.
   * @return null or header value from internal inbound key-value {@link #_headers}.
   */
  public String getHeader(byte[] k) {
    if (_headers == null) {
      return null;
    }
    int start = -1;
    for (int i = 0; i < _headers.length; ) {
      int j = 0;
      for (; j < k.length; j++) {
        if (k[j] != _headers[i++]) {
          break;
        }
      }
      if (j == k.length) {
        checkState(EQ == _headers[i]); // check that "=" was matched.
        start = i + 1; // match beginning of the value.
        break;
      }
    }
    if (start != -1) {
      if (start == _headers.length) {
        return "";
      }
      int end = _headers.length;
      for (int i = start; i < end; i++) {
        // find "," or the end of string.
        if (COMMA == _headers[i]) {
          end = i;
          break;
        }
      }
      return new String(Arrays.copyOfRange(_headers, start, end));
    }
    return null;
  }

  public byte[] asBinary() {
    if (_map.isEmpty()) {
      return EMPTY_FRAME;
    }

    Set<Map.Entry<String, String>> entries = _map.entrySet();
    int mapSize = entries.size();
    int equalSignsNum = mapSize;
    int commaSignsNum = mapSize - 1;

    byte[] headers = new byte[0];
    if (_headers != null && _headers.length > 0) {
      ByteBuffer buffer = ByteBuffer.allocate(mapSize > 0 ? _headers.length + 1 : _headers.length);
      buffer.put(_headers);
      if (mapSize > 0) {
        buffer.put(COMMA);
      }
      headers = buffer.array();
    }

    int i = 0;
    CharBuffer buffer = CharBuffer.allocate(_mapCharCounter + equalSignsNum + commaSignsNum);
    for (Map.Entry<String, String> entry : entries) {
      buffer.put(entry.getKey());
      buffer.put("=");
      buffer.put(entry.getValue());
      if (i != mapSize - 1) {
        buffer.put(",");
      }
      i++;
    }
    byte[] map = Charset.forName("ISO-8859-1").encode((CharBuffer) buffer.flip()).array();

    byte[] bin = new byte[headers.length + map.length];
    System.arraycopy(headers, 0, bin, 0, headers.length);
    System.arraycopy(map, 0, bin, headers.length, map.length);

    return bin;
  }
}
