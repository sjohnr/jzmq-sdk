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

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

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
@SuppressWarnings("unchecked")
public class ZmqHeaders<T extends ZmqHeaders> {

  private static final Pattern commaSign = Pattern.compile(",");
  private static final Pattern equalSign = Pattern.compile("=");

  private final Map<String, String> _map = new LinkedHashMap<String, String>();
  private int _charCounter = 0;

  //// METHODS

  public final T copy(ZmqHeaders headers) {
    for (Map.Entry<String, String> entry : (Set<Map.Entry<String, String>>) headers._map.entrySet()) {
      set(entry.getKey(), entry.getValue());
    }
    return (T) this;
  }

  public final T copy(byte[] headers) {
    if (isEmptyFrame(headers)) {
      return (T) this;
    }

    for (String pair : commaSign.split(new String(headers))) {
      String[] kv = equalSign.split(pair);
      set(kv[0], kv.length == 1 ? "" : kv[1]);
    }

    return (T) this;
  }

  public final T set(String k, String v) {
    checkArgument(k != null);
    checkArgument(v != null);

    String prev_v = _map.put(k, v);
    if (prev_v != null) {
      _charCounter += (v.length() - prev_v.length());
    }
    else {
      _charCounter += (k.length() + v.length());
    }

    return (T) this;
  }

  /**
   * @param k header id.
   * @return removed header content. <b>Null if there's no header by given id.</b>
   */
  public final String remove(String k) {
    String v = _map.remove(k);
    if (v != null) {
      _charCounter -= (k.length() + v.length());
    }
    return v;
  }

  /**
   * @param k header id.
   * @return header content. <b>Null if there's no header by given id.</b>
   */
  public final String getHeaderOrNull(String k) {
    return _map.get(k);
  }

  /**
   * @param k header id.
   * @return header content. <b>Never null.</b>
   */
  public final String getHeaderOrException(String k) {
    String v = getHeaderOrNull(k);
    if (v == null) {
      throw ZmqException.headerIsNotSet();
    }
    return v;
  }

  /**
   * Converts headers to "headers" string.
   *
   * @return "headers" string.
   */
  public final byte[] asBinary() {
    if (_map.isEmpty()) {
      return EMPTY_FRAME;
    }

    Set<Map.Entry<String, String>> entries = _map.entrySet();
    int entriesSize = entries.size();
    int equalSignsNum = entriesSize;
    int commaSignsNum = entriesSize - 1;
    CharBuffer buffer = CharBuffer.allocate(_charCounter + equalSignsNum + commaSignsNum);
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
