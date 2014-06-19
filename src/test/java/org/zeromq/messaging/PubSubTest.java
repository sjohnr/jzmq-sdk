package org.zeromq.messaging;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.zeromq.messaging.ZmqMessage.EMPTY_FRAME;

public class PubSubTest extends ZmqAbstractTest {

  static final Logger LOG = LoggerFactory.getLogger(PubSubTest.class);

  static class Fixture {

    final ZmqContext ctx;

    Fixture(ZmqContext ctx) {
      this.ctx = ctx;
    }

    ZmqChannel newPublisher() {
      return ZmqChannel.PUB(ctx).withProps(Props.builder().withBindAddr(inprocAddr("PubSubTest")).build()).build();
    }

    ZmqChannel newSubscriber() {
      return ZmqChannel.SUB(ctx).withProps(Props.builder().withConnectAddr(inprocAddr("PubSubTest")).build()).build();
    }
  }

  @Test
  public void t0() throws InterruptedException {
    LOG.info("Testing .send()/.recv() .");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      byte[] topicA = "A".getBytes();
      byte[] topicB = "B".getBytes();
      byte[] topicC = "C".getBytes();

      subscriber.subscribe(topicA);
      subscriber.subscribe(topicB);
      subscriber.subscribe(topicC);

      // send message with topic A.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicA).build());
      // send message with topic B.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicB).build());
      // send message with topic C.
      publisher.send(ZmqMessage.builder(HELLO()).withTopic(topicC).build());

      // send other messages.
      for (int i = 0; i < 3; i++) {
        publisher.send(ZmqMessage.builder(HELLO()).withTopic(EMPTY_FRAME).build());
      }

      // receive three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // assert that other messages can't be received because of topic subscription.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }

  @Test
  public void t1() throws InterruptedException {
    LOG.info("Testing subscribtions and .send()/.recv() .");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      byte[] topic = "topic".getBytes();
      // subscribe just once.
      subscriber.subscribe(topic);

      // send messages with topic.
      for (int i = 0; i < 4; i++) {
        publisher.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      }

      // receive only three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // assert that messages can't be received.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }

  @Test
  public void t2() throws InterruptedException {
    LOG.info("Testing subscribtions.");

    Fixture f = new Fixture(ctx());
    ZmqChannel publisher = f.newPublisher();
    ZmqChannel subscriber = f.newSubscriber();

    try {
      // subscribe many times.
      int numOfSubscr = 17;
      // topic.
      byte[] topic = "topic".getBytes();
      for (int i = 0; i < numOfSubscr; i++) {
        subscriber.subscribe(topic);
      }

      // send messages with topic.
      for (int i = 0; i < 6; i++) {
        publisher.send(ZmqMessage.builder(HELLO()).withTopic(topic).build());
      }

      // receive only three messages.
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      assert subscriber.recv() != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // check that you can get messages because subscriptions are there yet.
      assert subscriber.recv() != null;
      // unsubscribe again.
      subscriber.unsubscribe(topic);
      // check again.
      assert subscriber.recv() != null;
      // unsubscribe fully.
      for (int i = 0; i < numOfSubscr; i++) {
        subscriber.unsubscribe(topic);
      }
      // assert that messages can't be received.
      assert subscriber.recv() == null;
    }
    finally {
      subscriber.destroy();
      publisher.destroy();
    }
  }
}
