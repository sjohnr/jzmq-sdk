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

import com.google.common.base.Preconditions;
import org.zeromq.support.ObjectAdapter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class VersionConverter implements ObjectAdapter<String, byte[]> {

  private static final Pattern PATTERN_VERSION =
      Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})");

  @Override
  public byte[] convert(String src) {
    Matcher matcher = PATTERN_VERSION.matcher(src);
    Preconditions.checkState(matcher.matches(), "wrong version format: " + src);
    return versionAsBytes(matcher.group(1)/* version */);
  }

  private byte[] versionAsBytes(String version) {
    String[] s = version.split("\\.");
    byte major = Byte.parseByte(s[0]);
    byte minor = Byte.parseByte(s[1]);
    byte patch = Byte.parseByte(s[2]);
    return new byte[]{major, minor, patch};
  }
}
