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
import java.util.LinkedHashMap;
import java.util.Map;

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
 * Where header {@code .._id .._value} are strings. Wire format is JSON.
 */
@SuppressWarnings("unchecked")
public class ZmqHeaders<T extends ZmqHeaders> {

  private final LinkedHashMap<String, String> _map = new LinkedHashMap();

  private static final JsonFactory DEFAULT_JSON_FACTORY = new JsonFactory();

  static {
    DEFAULT_JSON_FACTORY.disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES);
    DEFAULT_JSON_FACTORY.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
    DEFAULT_JSON_FACTORY.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
    DEFAULT_JSON_FACTORY.enable(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS);
  }

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
      JsonParser parser = DEFAULT_JSON_FACTORY.createParser(headers);
      JsonToken token;
      String headerId = null;
      do {
        token = parser.nextToken();
        if (token != null) {
          switch (token) {
            case FIELD_NAME:
              String fn = parser.getText();
              if (headerId == null || headerId.isEmpty()) {
                headerId = fn;
              }
              break;
            case VALUE_STRING:
              String text = parser.getText();
              if (text == null || text.isEmpty()) {
                throw ZmqException.wrongHeader();
              }
              _map.put(headerId, text);
              headerId = null;
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
   * Converts headers to JSON.
   *
   * @return JSON string.
   */
  public final byte[] asBinary() {
    if (_map.isEmpty()) {
      return ZmqMessage.EMPTY_FRAME;
    }
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = DEFAULT_JSON_FACTORY.createGenerator(writer);
      gen.writeStartObject();
      for (Map.Entry<String, String> entry : _map.entrySet()) {
        gen.writeStringField(entry.getKey(), entry.getValue());
      }
      gen.writeEndObject();
      gen.close();
      return writer.toString().getBytes();
    }
    catch (IOException e) {
      throw ZmqException.seeCause(e);
    }
  }
}
