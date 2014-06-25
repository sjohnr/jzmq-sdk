package org.zeromq.messaging.device.chat;

import org.junit.Test;
import org.zeromq.messaging.Props;
import org.zeromq.messaging.ZmqAbstractTest;
import org.zeromq.messaging.ZmqChannel;

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
      ZmqChannel pub = ZmqChannel.PUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("p>>")).build()).build();
      ZmqChannel sub = ZmqChannel.SUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("s<<")).build()).build();

      byte[] topic = "xxx".getBytes();
      sub.subscribe(topic);

      waitSec(); // wait a second.

      pub.pub(topic, emptyHeaders(), payload(), 0);
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
      ZmqChannel galaSays = ZmqChannel.PUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("gala>>")).build()).build();
      ZmqChannel galaListens = ZmqChannel.SUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("gala<<")).build()).build();

      ZmqChannel alenkaSays = ZmqChannel.PUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("alenka>>")).build()).build();
      ZmqChannel alenkaListens = ZmqChannel.SUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("alenka<<")).build()).build();

      byte[] topic = "xxx".getBytes();
      galaListens.subscribe(topic);
      alenkaListens.subscribe(topic);

      waitSec(); // wait a second.

      galaSays.pub(topic, emptyHeaders(), payload(), 0);
      alenkaSays.pub(topic, emptyHeaders(), payload(), 0);

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

    f.chat(inprocAddr("p>>"),
           inprocAddr("p>>>>"),
           inprocAddr("s<<"),
           inprocAddr("p>>>>"));

    f.init();

    try {
      ZmqChannel pub = ZmqChannel.PUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("p>>")).build()).build();
      ZmqChannel sub = ZmqChannel.SUB(ctx()).withProps(Props.builder().withConnectAddr(inprocAddr("s<<")).build()).build();

      byte[] topicXXX = "xxx".getBytes();

      sub.subscribe(topicXXX); // subscribe first time.
      sub.subscribe(topicXXX); // subscribe second time.

      waitSec(); // wait a second.

      pub.pub(topicXXX, emptyHeaders(), payload(), 0);
      assert sub.recv(0) != null;

      // unsubscribe first time.
      sub.unsubscribe(topicXXX);
      // ensure that you still get message since one subscription remains.
      pub.pub(topicXXX, emptyHeaders(), emptyPayload(), 0);

      // unsubscribe last time.
      sub.unsubscribe(topicXXX);
      // ensure that you will not receive a message since all subscriptions are unsubscribed.
      pub.pub(topicXXX, emptyHeaders(), emptyPayload(), 0);
      assert sub.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }
}
