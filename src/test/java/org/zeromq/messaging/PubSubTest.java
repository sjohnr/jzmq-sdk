package org.zeromq.messaging;

import org.junit.Test;

import static org.zeromq.messaging.ZmqFrames.EMPTY_FRAME;

public class PubSubTest extends ZmqAbstractTest {

  @Test
  public void t0() throws InterruptedException {
    PubSubFixture f = new PubSubFixture(c());
    ZmqChannel publisher = f.publisher();
    ZmqChannel subscriber = f.subscriber();

    try {
      byte[] topicA = "A".getBytes();
      byte[] topicB = "B".getBytes();
      byte[] topicC = "C".getBytes();

      subscriber.subscribe(topicA);
      subscriber.subscribe(topicB);
      subscriber.subscribe(topicC);

      // send message with topic A.
      publisher.pub(topicA, payload(), 0);
      // send message with topic B.
      publisher.pub(topicB, payload(), 0);
      // send message with topic C.
      publisher.pub(topicC, payload(), 0);

      // send other messages.
      for (int i = 0; i < 3; i++) {
        publisher.pub(EMPTY_FRAME/*empty topic*/, payload(), 0);
      }

      // receive three messages.
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      // assert that other messages can't be received because of topic subscription.
      assert subscriber.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t1() throws InterruptedException {
    PubSubFixture f = new PubSubFixture(c());
    ZmqChannel publisher = f.publisher();
    ZmqChannel subscriber = f.subscriber();

    try {
      byte[] topic = "topic".getBytes();
      // subscribe just once.
      subscriber.subscribe(topic);

      // send messages with topic.
      for (int i = 0; i < 4; i++) {
        publisher.pub(topic, payload(), 0);
      }

      // receive only three messages.
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // assert that messages can't be received.
      assert subscriber.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }

  @Test
  public void t2() throws InterruptedException {
    PubSubFixture f = new PubSubFixture(c());
    ZmqChannel publisher = f.publisher();
    ZmqChannel subscriber = f.subscriber();

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
        publisher.pub(topic, payload(), 0);
      }

      // receive only three messages.
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      assert subscriber.recv(0) != null;
      // unsubscribe just once.
      subscriber.unsubscribe(topic);
      // check that you can get messages because subscriptions are there yet.
      assert subscriber.recv(0) != null;
      // unsubscribe again.
      subscriber.unsubscribe(topic);
      // check again.
      assert subscriber.recv(0) != null;
      // unsubscribe fully.
      for (int i = 0; i < numOfSubscr; i++) {
        subscriber.unsubscribe(topic);
      }
      // assert that messages can't be received.
      assert subscriber.recv(0) == null;
    }
    finally {
      f.destroy();
    }
  }
}
