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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
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

  /** Outbound key-value string map. */
  private final Map<String, String> _map = new LinkedHashMap<String, String>();
  /** Helper attribute for optimal building binary format of {@link #_map}. */
  private int _mapCharCounter = 0;
  /** Inbound key-value comma separated string. */
  private String _headers;

  public static final class Builder implements ObjectBuilder<ZmqHeaders> {

    private final ZmqHeaders _target = new ZmqHeaders();

    private Builder() {
      // no-op.
    }

    private Builder(byte[] headers) {
      checkArgument(headers != null);
      _target._headers = new String(headers);
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
   * @param k header name or id.
   * @return null or header value from internal outbound {@link #_map}.
   */
  public String getHeader(String k) {
    return _map.get(k);
  }

  /**
   * Getting inbound header value by header name.
   *
   * @param pair regexp pattern representing match for header name and value.
   * @return null or header value from internal inbound key-value {@link #_headers}.
   */
  public String getHeader(Pattern pair) {
    if (_headers == null) {
      return null;
    }
    Matcher m = pair.matcher(_headers);
    if (!m.find()) {
      return null;
    }
    return m.group(1);
  }

  public byte[] asBinary() {
    if (_map.isEmpty()) {
      return EMPTY_FRAME;
    }

    Set<Map.Entry<String, String>> entries = _map.entrySet();
    int entriesSize = entries.size();
    int equalSignsNum = entriesSize;
    int commaSignsNum = entriesSize - 1;

    // if inbound headers are present -- then add them as well.
    int headersLen = 0;
    if (_headers != null && _headers.length() > 0) {
      headersLen = _headers.length() + 1 /* one more comma character */;
    }
    CharBuffer buffer = CharBuffer.allocate(headersLen + _mapCharCounter + equalSignsNum + commaSignsNum);
    if (_headers != null) {
      buffer.put(_headers);
      if (entriesSize > 0) {
        buffer.put(",");
      }
    }
    int i = 0;
    for (Map.Entry<String, String> entry : entries) {
      buffer.put(entry.getKey());
      buffer.put("=");
      buffer.put(entry.getValue());
      if (i != entriesSize - 1) {
        buffer.put(",");
      }
      i++;
    }

    return Charset.forName("ISO-8859-1").encode((CharBuffer) buffer.flip()).array();
  }
}
