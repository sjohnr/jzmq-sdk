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

import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqContext;
import org.zeromq.messaging.ZmqMessage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class Run {

  public static void main(String[] args) throws InterruptedException, UnknownHostException {
    final ZmqContext ctx = new ZmqContext();
    ctx.setThreadNum(1);
    ctx.init();

    final String address = "epgm://" + InetAddress.getLocalHost().getHostAddress() + ";224.0.0.1:45577";
    final byte[] topic = "xyz".getBytes();

    try {

      Thread t0 = new Thread() {
        @Override
        public void run() {
          ZmqChannel channel = ZmqChannel.builder()
                                         .withZmqContext(ctx)
                                         .ofPUBType()
                                         .withBindAddress(address)
                                         .build();

          while (true) {
            ZmqMessage hello = ZmqMessage.builder()
                                         .withTopic(topic)
                                         .withPayload("hello".getBytes())
                                         .build();
            boolean send = channel.send(hello);
            if (!send) {
              System.out.println("Can't send via PUB! Exiting JVM.");
              System.exit(-1);
            }
            try {
              TimeUnit.MILLISECONDS.sleep(500);
            }
            catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      t0.start();

      Thread t1 = new Thread() {
        @Override
        public void run() {
          ZmqChannel channel = ZmqChannel.builder()
                                         .withZmqContext(ctx)
                                         .ofSUBType()
                                         .withConnectAddress(address)
                                         .build();

          channel.subscribe(topic);

          while (true) {
            ZmqMessage recv = channel.recv();
            if (recv != null) {
              System.out.println(">> get HELLO!");
            }
            try {
              TimeUnit.MILLISECONDS.sleep(250);
            }
            catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
      t1.start();

      t0.join();
      t1.join();
    }
    finally {
      ctx.destroy();
    }
  }
}
