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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PropsTest {

  @Test
  public void t0() {
    Props props = Props.builder().build();
    props.setBindAddr("tcp://*:9090");
    assertEquals(1, props.bindAddr().size());
    assertEquals("tcp://*:9090", props.bindAddr().get(0));

    props = Props.builder().build();
    props.setBindAddr("\ttcp://*:9090\ttcp://*:7676\t\n");
    assertEquals(2, props.bindAddr().size());
    assertEquals("tcp://*:9090", props.bindAddr().get(0));
    assertEquals("tcp://*:7676", props.bindAddr().get(1));

    props = Props.builder().build();
    props.setConnectAddr("\ntcp://localhost:8000,\n" + "epgm://10.0.0.13;239.192.1.1:3055,\n" + "tcp://localhost:7000");
    assertEquals(3, props.connectAddr().size());
    assertEquals("tcp://localhost:8000", props.connectAddr().get(0));
    assertEquals("epgm://10.0.0.13;239.192.1.1:3055", props.connectAddr().get(1));
    assertEquals("tcp://localhost:7000", props.connectAddr().get(2));

    props = Props.builder().build();
    props.setConnectAddr("\n" +
                         "    \ttcp://localhost:8000 \n" +
                         "    \tepgm://10.0.0.13;239.192.1.1:3055, \n" +
                         "    \ttcp://localhost:7000 \n" +
                         "\n");
    assertEquals(3, props.connectAddr().size());
    assertEquals("tcp://localhost:8000", props.connectAddr().get(0));
    assertEquals("epgm://10.0.0.13;239.192.1.1:3055", props.connectAddr().get(1));
    assertEquals("tcp://localhost:7000", props.connectAddr().get(2));
  }
}
