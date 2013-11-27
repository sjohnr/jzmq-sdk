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

import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Run {

  public static void main(final String[] args) throws InterruptedException, UnknownHostException {
    final ZmqContext ctx = new ZmqContext();
    ctx.setThreadNum(1);
    ctx.init();

    String nic = args[0];
    final String prefix = args[1];
    final long pubRate = Long.valueOf(args[2]);
    final long subRate = Long.valueOf(args[3]);
    final String address = "epgm://" + nic + ";224.0.0.1:45577";
    final byte[] topic = "xyz".getBytes();

    ExecutorService exec = Executors.newCachedThreadPool();

    // publisher
    exec.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ZmqChannel channel = ZmqChannel.builder()
                                       .withZmqContext(ctx)
                                       .ofPUBType()
                                       .withBindAddress(address)
                                       .build();
        while (true) {
          ZmqMessage hello = ZmqMessage.builder()
                                       .withTopic(topic)
                                       .withPayload(("hello" + prefix + System.nanoTime()).getBytes())
                                       .build();
          boolean send = channel.send(hello);
          if (!send) {
            System.out.println("Can't send message via PUB! Thank you. Good bye.");
            System.exit(-1);
          }
          TimeUnit.MILLISECONDS.sleep(pubRate);
        }
      }
    });

    // subscriber
    exec.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        ZmqChannel channel = ZmqChannel.builder()
                                       .withZmqContext(ctx)
                                       .ofSUBType()
                                       .withConnectAddress(address)
                                       .build();

        channel.subscribe(topic);

        while (true) {
          ZmqMessage recv = channel.recv();
          if (recv != null) {
            System.out.println(">> got message: " + new String(recv.payload()));
          }
          TimeUnit.MILLISECONDS.sleep(subRate);
        }
      }
    });

    // block forever
    exec.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }
}
