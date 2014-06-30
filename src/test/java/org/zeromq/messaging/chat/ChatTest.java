package org.zeromq.messaging.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;

public class ChatTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inproc("p>>"),
           inproc("p>>>>"),
           inproc("s<<"),
           inproc("p>>>>"));

    f.init();
    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx()).with(Props.builder().withConnectAddr(inproc("p>>")).build()).build();
      ZmqChannel sub = ZmqChannel.SUB(ctx()).with(Props.builder().withConnectAddr(inproc("s<<")).build()).build();

      byte[] topic = "xxx".getBytes();
      sub.subscribe(topic);

      waitSec(); // wait a second.

      pub.pub(topic, payload(), 0);
      assert sub.recv(0) != null;
      assert sub.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t1() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inproc("gala>>"),
           bind(4040),
           inproc("gala<<"),
           conn(5050));

    f.chat(inproc("alenka>>"),
           bind(5050),
           inproc("alenka<<"),
           conn(4040));

    f.init();
    try {
      ZmqChannel galaSays = ZmqChannel.PUB(ctx()).with(Props.builder().withConnectAddr(inproc("gala>>")).build()).build();
      ZmqChannel galaListens = ZmqChannel.SUB(ctx()).with(Props.builder().withConnectAddr(inproc("gala<<")).build()).build();

      ZmqChannel alenkaSays = ZmqChannel.PUB(ctx()).with(Props.builder().withConnectAddr(inproc("alenka>>")).build()).build();
      ZmqChannel alenkaListens = ZmqChannel.SUB(ctx()).with(Props.builder().withConnectAddr(inproc("alenka<<")).build()).build();

      byte[] topic = "xxx".getBytes();
      galaListens.subscribe(topic);
      alenkaListens.subscribe(topic);

      waitSec(); // wait a second.

      galaSays.pub(topic, payload(), 0);
      alenkaSays.pub(topic, payload(), 0);

      assert galaListens.recv(0) != null;
      assert alenkaListens.recv(0) != null;

      assert galaListens.recv(0) == null;
      assert alenkaListens.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t2() throws InterruptedException {
    ChatFixture f = new ChatFixture(ctx());

    f.chat(inproc("p>>"),
           inproc("p>>>>"),
           inproc("s<<"),
           inproc("p>>>>"));

    f.init();
    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx()).with(Props.builder().withConnectAddr(inproc("p>>")).build()).build();
      ZmqChannel sub = ZmqChannel.SUB(ctx()).with(Props.builder().withConnectAddr(inproc("s<<")).build()).build();

      byte[] topicXXX = "xxx".getBytes();

      sub.subscribe(topicXXX); // subscribe first time.
      sub.subscribe(topicXXX); // subscribe second time.

      waitSec(); // wait a second.

      pub.pub(topicXXX, payload(), 0);
      assert sub.recv(0) != null;

      // unsubscribe first time.
      sub.unsubscribe(topicXXX);
      // ensure that you still get message since one subscription remains.
      pub.pub(topicXXX, emptyPayload(), 0);

      // unsubscribe last time.
      sub.unsubscribe(topicXXX);
      // ensure that you will not receive a message since all subscriptions are unsubscribed.
      pub.pub(topicXXX, emptyPayload(), 0);
      assert sub.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }
}
