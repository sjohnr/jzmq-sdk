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

package org.zeromq.messaging.device.service;

import java.util.Comparator;

public class VersionRangeComparator implements Comparator<byte[]> {

  @Override
  public int compare(byte[] version, byte[] range) {
    int input = asInt(version[0], version[1], version[2]);
    int min = asInt(range[0], range[1], range[2]);
    int max = asInt(range[3], range[4], range[5]);

    return input >= min && input <= max ? 0 : -1;
  }

  private int asInt(byte major, byte minor, byte patch) {
    return (int) (major * 1e6 + minor * 1e3 + patch);
  }
}
