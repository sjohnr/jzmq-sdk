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

package org.zeromq;

import java.util.ArrayList;
import java.util.List;

public class Receiver {

  static final byte[] PING = "ping".getBytes();

  public static void main(String[] args) throws InterruptedException {
    Thread t0 = new Thread(new Runnable() {
      @Override
      public void run() {

        ZMQ.Context ctx = ZMQ.context(1);
        ZMQ.Poller poller = new ZMQ.Poller(1);
        ZMQ.Socket frontend = createFrontend(ctx);

        poller.register(frontend, ZMQ.Poller.POLLIN);

        for (; ; ) {
          poller.poll(1000);
          // if got incoming traffic -- recv it and send back; incr counter.
          if (poller.pollin(0)) {
            for (; ; ) {
              // .recv()
              List<byte[]> msg = new ArrayList<byte[]>();
              if (recv(frontend, msg) == 0) {
                break;
              }
              // .send()
              send(frontend, msg);
            }
          }
          // if no incoming traffic on frontend -- send a PING.
          else {
            sendPing(frontend);
            System.out.println("Sent PING");
          }
        }
      }
    });
    t0.start();
    t0.join();
  }

  static ZMQ.Socket createFrontend(ZMQ.Context ctx) {
    ZMQ.Socket frontend = ctx.socket(ZMQ.DEALER);
    frontend.setSendTimeOut(-1);
    frontend.setRcvHWM(0); // the limit is only processing power.
    frontend.connect("tcp://localhost:6060");
    return frontend;
  }

  static void send(ZMQ.Socket frontend, List<byte[]> msg) {
    List<byte[]> msgCopy = new ArrayList<byte[]>();
    msgCopy.add(new byte[0]); // empty frame for DEALER
    msgCopy.add(msg.get(1/*PAYLOAD*/));
    int i = 0;
    for (byte[] frame : msgCopy) {
      frontend.send(frame, ++i < msgCopy.size() ? ZMQ.SNDMORE : ZMQ.DONTWAIT);
    }
  }

  static int recv(ZMQ.Socket frontend, List<byte[]> msg) {
    for (; ; ) {
      byte[] f = frontend.recv(ZMQ.DONTWAIT);
      if (f == null) {
        return 0;
      }
      msg.add(f);
      if (!frontend.hasReceiveMore()) {
        return 1;
      }
    }
  }

  static void sendPing(ZMQ.Socket frontend) {
    List<byte[]> msg = new ArrayList<byte[]>();
    msg.add(new byte[0]); // empty frame for DEALER
    msg.add(PING);
    int i = 0;
    for (byte[] frame : msg) {
      frontend.send(frame, ++i < msg.size() ? ZMQ.SNDMORE : ZMQ.DONTWAIT);
    }
  }
}
