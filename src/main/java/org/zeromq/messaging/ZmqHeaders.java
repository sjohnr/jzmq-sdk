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
import com.google.common.base.Strings;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.zeromq.support.ZmqUtils.isDivFrame;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

public class ZmqHeaders {

  /** Internal mapping between header id and its content. */
  private final Multimap<String, String> _map = LinkedHashMultimap.create();

  //// METHODS

  public final ZmqHeaders copy(ZmqHeaders headers) {
    _map.putAll(headers._map);
    return this;
  }

  public final ZmqHeaders copy(byte[] headers) {
    if (isEmptyFrame(headers)) {
      return this;
    }
    assert !isDivFrame(headers);
    try {
      JsonFactory jf = new JsonFactory();
      JsonParser p = jf.createParser(headers);
      JsonToken token;
      String headerId = null;
      Collection<String> headerContent = null;
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
              assert !Strings.isNullOrEmpty(text);
              headerContent.add(text);
              break;
            case END_ARRAY:
              _map.putAll(headerId, headerContent);
              headerId = null;
              headerContent = null;
              break;
          }
        }
      }
      while (token != null);
    }
    catch (IOException e) {
      throw ZmqException.wrap(e);
    }
    return this;
  }

  public final ZmqHeaders set(String headerId, String headerContent) {
    assert !Strings.isNullOrEmpty(headerId);
    assert !Strings.isNullOrEmpty(headerContent);
    remove(headerId);
    _map.put(headerId, headerContent);
    return this;
  }

  public final ZmqHeaders set(String headerId, Number headerContent) {
    assert !Strings.isNullOrEmpty(headerId);
    assert headerContent != null;
    remove(headerId);
    _map.put(headerId, headerContent.toString());
    return this;
  }

  /**
   * @param id header id.
   * @return removed header content. <b>Null if there's not such header with given id.</b>
   */
  public final Collection<String> remove(String id) {
    return _map.removeAll(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Null if there's not such header with given id.</b>
   */
  public final Collection<String> getHeaderOrNull(String id) {
    return _map.get(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Never null.</b>
   * @throws ZmqException in case header with given id is absent.
   */
  public final Collection<String> getHeaderOrException(String id) throws ZmqException {
    Collection<String> content = getHeaderOrNull(id);
    if (content.isEmpty()) {
      throw ZmqException.headerIsNotSet();
    }
    return content;
  }

  public final byte[] asBinary() {
    Map<String, Collection<String>> m = _map.asMap();
    if (m.isEmpty()) {
      return new byte[0];
    }
    try {
      JsonFactory jf = new JsonFactory();
      StringWriter w = new StringWriter();
      JsonGenerator g = jf.createGenerator(w);
      g.writeStartObject();
      {
        for (Map.Entry<String, Collection<String>> entry : m.entrySet()) {
          String headerId = entry.getKey();
          Collection<String> headerContent = entry.getValue();
          g.writeArrayFieldStart(headerId);
          {
            for (String c : headerContent) {
              g.writeString(c);
            }
          }
          g.writeEndArray();
        }
      }
      g.writeEndObject();
      g.close();
      return w.toString().getBytes();
    }
    catch (IOException e) {
      throw ZmqException.wrap(e);
    }
  }
}
