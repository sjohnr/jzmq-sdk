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

package org.zeromq.messaging.device.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;
import org.zeromq.messaging.ZmqMessage;

public class ChatTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inprocAddr("p>>"),
           inprocAddr("p>>>>"),
           inprocAddr("s<<"),
           inprocAddr("p>>>>"));

    f.init();

    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnect(inprocAddr("p>>"))
                                                 .build())
                                 .build();

      ZmqChannel sub = ZmqChannel.SUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnect(inprocAddr("s<<"))
                                                 .build())
                                 .build();

      byte[] topic = "xxx".getBytes();
      sub.subscribe(topic);

      waitSec(); // wait a second.

      pub.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      assertPayload("hello", sub.recv());

      assert sub.recv() == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t1() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inprocAddr("gala>>"),
           bindAddr(4040),
           inprocAddr("gala<<"),
           connAddr(5050));

    f.chat(inprocAddr("alenka>>"),
           bindAddr(5050),
           inprocAddr("alenka<<"),
           connAddr(4040));

    f.init();

    try {
      ZmqChannel galaSays = ZmqChannel.PUB(ctx())
                                      .withProps(Props.builder()
                                                      .withConnect(inprocAddr("gala>>"))
                                                      .build())
                                      .build();

      ZmqChannel galaListens = ZmqChannel.SUB(ctx())
                                         .withProps(Props.builder()
                                                         .withConnect(inprocAddr("gala<<"))
                                                         .build())
                                         .build();

      ZmqChannel alenkaSays = ZmqChannel.PUB(ctx())
                                        .withProps(Props.builder()
                                                        .withConnect(inprocAddr("alenka>>"))
                                                        .build())
                                        .build();

      ZmqChannel alenkaListens = ZmqChannel.SUB(ctx())
                                           .withProps(Props.builder()
                                                           .withConnect(inprocAddr("alenka<<"))
                                                           .build())
                                           .build();

      byte[] topic = "xxx".getBytes();
      galaListens.subscribe(topic);
      alenkaListens.subscribe(topic);

      waitSec(); // wait a second.

      galaSays.send(ZmqMessage.builder(CARP()).withTopic(topic).build());
      alenkaSays.send(ZmqMessage.builder(SHIRT()).withTopic(topic).build());

      assertPayload("shirt", galaListens.recv());
      assertPayload("carp", alenkaListens.recv());

      assert galaListens.recv() == null;
      assert alenkaListens.recv() == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t2() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inprocAddr("p>>"),
           inprocAddr("p>>>>"),
           inprocAddr("s<<"),
           inprocAddr("p>>>>"));

    f.init();

    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnect(inprocAddr("p>>"))
                                                 .build())
                                 .build();

      ZmqChannel sub = ZmqChannel.SUB(ctx())
                                 .withProps(Props.builder()
                                                 .withConnect(inprocAddr("s<<"))
                                                 .build())
                                 .build();

      byte[] topic = "xxx".getBytes();

      sub.subscribe(topic); // subscribe first time.
      sub.subscribe(topic); // subscribe second time.

      waitSec(); // wait a second.

      pub.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      assertPayload("hello", sub.recv());

      // unsubscribe first time.
      sub.unsubscribe(topic);
      // ensure that you still get message since one subscription remains.
      pub.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      assertPayload("hello", sub.recv());

      // unsubscribe last time.
      sub.unsubscribe(topic);
      // ensure that you will not receive a message since all subscriptions are unsubscribed.
      pub.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      assert sub.recv() == null;
    }
    finally {
      f.destroy();
    }
  }
}
