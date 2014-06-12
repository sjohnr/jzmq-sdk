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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.util.LinkedHashMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.zeromq.support.ZmqUtils.isDivFrame;
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

  private static final Splitter.MapSplitter DEFAULT_SPLITTER = Splitter.on(",").withKeyValueSeparator("=");
  private static final Joiner.MapJoiner DEFAULT_JOINER = Joiner.on(",").withKeyValueSeparator("=");

  private final LinkedHashMap<String, String> _map = new LinkedHashMap();

  //// METHODS

  public final T copy(ZmqHeaders headers) {
    _map.putAll(headers._map);
    return (T) this;
  }

  public final T copy(byte[] headers) {
    if (isEmptyFrame(headers)) {
      return (T) this;
    }
    if (isDivFrame(headers)) {
      throw ZmqException.wrongHeader();
    }
    _map.putAll(DEFAULT_SPLITTER.split(new String(headers)));
    return (T) this;
  }

  public final T set(String k, String v) {
    checkArgument(!isNullOrEmpty(k));
    checkArgument(v != null);
    _map.put(k, v);
    return (T) this;
  }

  public final T set(String k, Number v) {
    checkArgument(!isNullOrEmpty(k));
    checkArgument(v != null);
    _map.put(k, v.toString());
    return (T) this;
  }

  /**
   * @param k header id.
   * @return removed header content. <b>Null if there's no header by given id.</b>
   */
  public final String remove(String k) {
    return _map.remove(k);
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
    String header = getHeaderOrNull(k);
    if (header == null || header.isEmpty()) {
      throw ZmqException.headerIsNotSet();
    }
    return header;
  }

  /**
   * Converts headers to "headers" string.
   *
   * @return "headers" string.
   */
  public final byte[] asBinary() {
    if (_map.isEmpty()) {
      return ZmqMessage.EMPTY_FRAME;
    }
    return DEFAULT_JOINER.join(_map.entrySet()).getBytes();
  }
}
