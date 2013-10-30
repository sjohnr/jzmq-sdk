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

import org.zeromq.support.ZmqUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import static org.zeromq.support.ZmqUtils.isDivFrame;
import static org.zeromq.support.ZmqUtils.isEmptyFrame;

public class ZmqHeaders {

  /** Set to 4 as size of Integer. */
  private static final int HEADER_ID_SIZE = 4;
  /** Internal mapping between header id and its content. */
  private final Map<Integer, byte[]> _map = new TreeMap<Integer, byte[]>();

  //// METHODS

  public final ZmqHeaders copyOf(ZmqHeaders headers) {
    _map.putAll(headers._map);
    return this;
  }

  public final ZmqHeaders put(int id, byte[] content) {
    _map.put(id, content);
    return this;
  }

  public final ZmqHeaders put(ZmqFrames headers) {
    for (byte[] header : headers) {
      assert !isEmptyFrame(header);
      assert !isDivFrame(header);
      // parse incoming header:
      // first 4 bytes -- header id,
      // the rest -- header content.
      byte[] id = new byte[HEADER_ID_SIZE];
      System.arraycopy(header, 0, id, 0, id.length);
      byte[] content = new byte[header.length - HEADER_ID_SIZE];
      System.arraycopy(header, HEADER_ID_SIZE, content, 0, content.length);
      _map.put(ZmqUtils.bytesAsInt(id), content);
    }
    return this;
  }

  public final ZmqFrames asFrames() {
    // return outgoing header data:
    // first 4 bytes -- header id,
    // the rest -- header content.
    ZmqFrames target = new ZmqFrames();
    for (Map.Entry<Integer, byte[]> entry : _map.entrySet()) {
      byte[] id = ZmqUtils.intAsBytes(entry.getKey());
      byte[] content = entry.getValue();
      target.add(ZmqUtils.mergeBytes(Arrays.asList(id, content)));
    }
    return target;
  }

  /**
   * @param id header id.
   * @return removed header content. <b>Null if there's not such header with given id.</b>
   */
  public final byte[] remove(int id) {
    return _map.remove(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Null if there's not such header with given id.</b>
   */
  public final byte[] getHeaderOrNull(int id) {
    return _map.get(id);
  }

  /**
   * @param id header id.
   * @return header content. <b>Never null.</b>
   * @throws ZmqException in case header with given id is absent.
   */
  public final byte[] getHeaderOrException(int id) throws ZmqException {
    byte[] bytes = getHeaderOrNull(id);
    if (bytes == null) {
      throw ZmqException.headerIsNotSet();
    }
    return bytes;
  }
}
