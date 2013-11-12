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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class VersionRangeConverterTest {

  @Test(expected = IllegalStateException.class)
  public void t0() {
    VersionRangeConverter target = new VersionRangeConverter();
    target.convert("[x.y.z-f.a.b]");
  }

  @Test(expected = IllegalStateException.class)
  public void t1() {
    VersionRangeConverter target = new VersionRangeConverter();
    target.convert("[1.2.3      -       4.5.6]");
  }

  @Test(expected = NumberFormatException.class)
  public void t2() {
    VersionRangeConverter target = new VersionRangeConverter();
    target.convert("[1.2.3-128.128.128]");
  }

  @Test
  public void t3() {
    VersionRangeConverter target = new VersionRangeConverter();
    byte[] bytes = target.convert("[1.0.21-3.1.127]");
    assertEquals(6, bytes.length);
  }
}
