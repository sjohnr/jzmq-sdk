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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.zeromq.support.ZmqUtils.isDivFrame;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

/**
 * Map data structure aimed to provide generic <i>headers</i>:
 * <pre>
 *   header_id_0  [header_value_0]
 *   ...
 *   header_id_m  [header_value_m]
 * </pre>
 * Where header -Id/-Value are strings. Wire format is JSON.
 */
@SuppressWarnings("unchecked")
public class ZmqHeaders<T extends ZmqHeaders> {

  /** Internal mapping between header id and its content. */
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

    try {
      JsonFactory jf = new JsonFactory();
      JsonParser p = jf.createParser(headers);
      JsonToken token;
      String headerId = null;
      ArrayList<String> headerContent = null;
      do {
        token = p.nextToken();
        if (token != null) {
          switch (token) {
            case FIELD_NAME:
              String fn = p.getText();
              if (headerId == null) {
                headerId = fn;
                headerContent = new ArrayList<String>();
              }
              break;
            case VALUE_STRING:
              String text = p.getText();
              if (isNullOrEmpty(text)) {
                throw ZmqException.wrongHeader();
              }
              headerContent.add(text);
              break;
            case END_ARRAY:
              if (headerContent.isEmpty()) {
                throw ZmqException.wrongHeader();
              }
              _map.put(headerId, headerContent.get(0));
              headerId = null;
              headerContent = null;
              break;
          }
        }
      }
      while (token != null);
    }
    catch (IOException e) {
      throw ZmqException.seeCause(e);
    }
    return (T) this;
  }

  public final T set(String headerId, String headerContent) {
    checkArgument(!isNullOrEmpty(headerId));
    checkArgument(!isNullOrEmpty(headerContent));
    _map.put(headerId, headerContent);
    return (T) this;
  }

  public final T set(String headerId, Number headerContent) {
    checkArgument(!isNullOrEmpty(headerId));
    checkArgument(headerContent != null);
    _map.put(headerId, headerContent.toString());
    return (T) this;
  }

  /**
   * @param id header id.
   * @return removed header content. <b>Null if there's no header by given id.</b>
   */
  public final String remove(String id) {
    return _map.remove(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Null if there's no header by given id.</b>
   */
  public final String getHeaderOrNull(String id) {
    return _map.get(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Never null.</b>
   */
  public final String getHeaderOrException(String id) {
    String c = getHeaderOrNull(id);
    if (c == null) {
      throw ZmqException.headerIsNotSet();
    }
    return c;
  }

  /**
   * Converts headers to JSON.
   *
   * @return JSON string.
   */
  public final byte[] asBinary() {
    if (_map.isEmpty()) {
      return new byte[0];
    }
    try {
      JsonFactory jf = new JsonFactory();
      StringWriter w = new StringWriter();
      JsonGenerator g = jf.createGenerator(w);
      g.writeStartObject();
      {
        for (Map.Entry<String, String> entry : _map.entrySet()) {
          String headerId = entry.getKey();
          String headerContent = entry.getValue();
          g.writeArrayFieldStart(headerId);
          {
            g.writeString(headerContent);
          }
          g.writeEndArray();
        }
      }
      g.writeEndObject();
      g.close();
      return w.toString().getBytes();
    }
    catch (IOException e) {
      throw ZmqException.seeCause(e);
    }
  }
}
